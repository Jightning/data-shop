//! Byte-level CSV dialect from rules.md: only `\n` ends a line, a quote may only wrap a whole
//! field, no value holds a comma/quote/newline, empty line or wrong field count is malformed.

use crate::util::Bs;

/// Calls `f` for every line of `chunk` (lines end at `\n`; a final unterminated line counts).
#[inline]
pub fn for_lines(chunk: &'static [u8], mut f: impl FnMut(&'static [u8])) {
    let mut pos = 0;
    for nl in memchr::memchr_iter(b'\n', chunk) {
        f(&chunk[pos..nl]);
        pos = nl + 1;
    }
    if pos < chunk.len() {
        f(&chunk[pos..]);
    }
}

#[inline(always)]
fn unquote(s: &'static [u8]) -> Option<Bs> {
    // Called only when the line holds a quote somewhere.
    match memchr::memchr(b'"', s) {
        None => Some(Bs::new(s)),
        Some(0) if s.len() >= 2 && s[s.len() - 1] == b'"' => {
            let inner = &s[1..s.len() - 1];
            if memchr::memchr(b'"', inner).is_some() {
                None
            } else {
                Some(Bs::new(inner))
            }
        }
        _ => None,
    }
}

/// Calls `f` with the N unquoted fields of every well-formed line of `chunk`; malformed lines
/// (empty, wrong field count, a quote not wrapping a whole field) are skipped.
///
/// Delimiter and quote masks let the scanner count quotes without revisiting their positions.
#[inline]
pub fn for_records<const N: usize>(chunk: &'static [u8], f: impl FnMut(&[Bs; N])) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("popcnt") {
            // Both instruction sets were checked once for the whole chunk.
            unsafe { return for_records_avx2(chunk, f) };
        }
    }
    for_records_masked::<N, false>(chunk, f);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,popcnt")]
unsafe fn for_records_avx2<const N: usize>(chunk: &'static [u8], f: impl FnMut(&[Bs; N])) {
    if N == 4 || N == 7 {
        for_records_by_line(chunk, f);
    } else {
        for_records_masked::<N, true>(chunk, f);
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn store_commas(mut commas: u64, base: usize, ends: &mut [usize]) {
    for end in ends {
        *end = base + commas.trailing_zeros() as usize;
        commas &= commas - 1;
    }
}

// Consume one row at a time from the SIMD masks. Counting commas before walking
// their set bits rules out malformed rows and removes the delimiter type and
// field-capacity checks from the comma loop.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn for_records_by_line<const N: usize>(chunk: &'static [u8], mut f: impl FnMut(&[Bs; N])) {
    let mut line_start = 0;
    let mut nc = 0;
    let mut quote_count = 0;
    let mut ends = [0; N];
    let mut out = [Bs::EMPTY; N];

    for base in (0..chunk.len() / 64 * 64).step_by(64) {
        // The loop only visits complete 64-byte blocks, after AVX2 detection.
        let (mut commas, mut newlines, mut quotes) =
            unsafe { masks::<true>(chunk.as_ptr().add(base)) };
        while newlines != 0 {
            let pos = base + newlines.trailing_zeros() as usize;
            let remaining = newlines & (newlines - 1);
            let before = (newlines ^ remaining) - 1;
            newlines = remaining;

            let fields = commas & before;
            let total = nc + fields.count_ones() as usize;
            if total == N - 1 {
                store_commas(fields, base, &mut ends[nc..total]);
                ends[N - 1] = pos;
                let quotes_in_line = quote_count + (quotes & before).count_ones() as usize;

                let mut start = line_start;
                let valid = if quotes_in_line == 0 {
                    for (slot, end) in out.iter_mut().zip(ends) {
                        // The mask walk yields ascending comma indices and the newline,
                        // all within the chunk. The previous delimiter is outside the field.
                        *slot = Bs::new(unsafe { chunk.get_unchecked(start..end) });
                        start = end + 1;
                    }
                    true
                } else {
                    let mut wrapping_quotes = 0;
                    for (slot, end) in out.iter_mut().zip(ends) {
                        // Both byte indices are in the chunk: end points to a delimiter
                        // and start is at or before it, even for an empty field.
                        let wrapped = unsafe {
                            (*chunk.get_unchecked(start) == b'"')
                                & (*chunk.get_unchecked(end.saturating_sub(1)) == b'"')
                                & (end - start >= 2)
                        } as usize;
                        wrapping_quotes += 2 * wrapped;
                        *slot =
                            Bs::new(unsafe { chunk.get_unchecked(start + wrapped..end - wrapped) });
                        start = end + 1;
                    }
                    wrapping_quotes == quotes_in_line
                };
                if valid {
                    f(&out);
                }
            }

            line_start = pos + 1;
            nc = 0;
            quote_count = 0;
            commas ^= fields;
            quotes &= !before;
        }

        let total = nc + commas.count_ones() as usize;
        if total < N {
            store_commas(commas, base, &mut ends[nc..total]);
            nc = total;
        } else {
            // Excess delimiters make the rest of this row malformed.
            nc = N;
        }
        quote_count += quotes.count_ones() as usize;
    }

    // This includes any row spanning the last full block and the scalar tail.
    for_lines(&chunk[line_start..], |line| {
        if split(line, &mut out) {
            f(&out);
        }
    });
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn masks<const AVX2: bool>(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let mut commas = 0u64;
    let mut newlines = 0u64;
    let mut quotes = 0u64;
    if AVX2 {
        for i in [0, 32] {
            let v = _mm256_loadu_si256(p.add(i).cast());
            let mask = |b| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(b))) as u32 as u64;
            commas |= mask(b',' as i8) << i;
            newlines |= mask(b'\n' as i8) << i;
            quotes |= mask(b'"' as i8) << i;
        }
    } else {
        for i in [0, 16, 32, 48] {
            let v = _mm_loadu_si128(p.add(i).cast());
            let mask = |b| _mm_movemask_epi8(_mm_cmpeq_epi8(v, _mm_set1_epi8(b))) as u64;
            commas |= mask(b',' as i8) << i;
            newlines |= mask(b'\n' as i8) << i;
            quotes |= mask(b'"' as i8) << i;
        }
    }
    (commas, newlines, quotes)
}

#[inline(always)]
fn for_records_masked<const N: usize, const AVX2: bool>(
    chunk: &'static [u8],
    mut f: impl FnMut(&[Bs; N]),
) {
    let mut line_start = 0;
    let mut ends = [0; N];
    let mut nc = 0;
    let mut quote_count = 0;
    let mut out = [Bs::EMPTY; N];

    let mut emit = |start: usize, end, ends: &mut [usize; N], nc, quote_count| {
        if end == start || nc != N - 1 {
            return;
        }
        ends[N - 1] = end;
        let mut pos = start;
        let mut wrapping_quotes = 0;
        for (slot, &end) in out.iter_mut().zip(ends.iter()) {
            let field = &chunk[pos..end];
            *slot = if field.len() >= 2 && field[0] == b'"' && field[field.len() - 1] == b'"' {
                wrapping_quotes += 2;
                Bs::new(&field[1..field.len() - 1])
            } else {
                Bs::new(field)
            };
            pos = end + 1;
        }
        // Every quote must have been accounted for by a distinct pair of field endpoints.
        if wrapping_quotes == quote_count {
            f(&out);
        }
    };

    let mut base = 0;
    #[cfg(target_arch = "x86_64")]
    while base + 64 <= chunk.len() {
        // The loop bounds give both SIMD implementations 64 readable bytes.
        let (commas, newlines, mut quotes) = unsafe { masks::<AVX2>(chunk.as_ptr().add(base)) };
        let mut delimiters = commas | newlines;
        while delimiters != 0 {
            let bit = delimiters.trailing_zeros();
            let pos = base + bit as usize;
            let remaining = delimiters & (delimiters - 1);
            let delimiter = delimiters ^ remaining;
            let lower = delimiter - 1;
            if newlines & delimiter != 0 {
                quote_count += (quotes & lower).count_ones() as usize;
                quotes &= !lower;
                emit(line_start, pos, &mut ends, nc, quote_count);
                line_start = pos + 1;
                nc = 0;
                quote_count = 0;
            } else {
                if nc < N - 1 {
                    ends[nc] = pos;
                }
                nc += 1;
            }
            delimiters = remaining;
        }
        quote_count += quotes.count_ones() as usize;
        base += 64;
    }
    for (i, &byte) in chunk[base..].iter().enumerate() {
        let pos = base + i;
        match byte {
            b',' => {
                if nc < N - 1 {
                    ends[nc] = pos;
                }
                nc += 1;
            }
            b'"' => quote_count += 1,
            b'\n' => {
                emit(line_start, pos, &mut ends, nc, quote_count);
                line_start = pos + 1;
                nc = 0;
                quote_count = 0;
            }
            _ => {}
        }
    }
    if line_start < chunk.len() {
        emit(line_start, chunk.len(), &mut ends, nc, quote_count);
    }
}

/// Splits `line` into exactly N fields (unquoted). Returns false if the line is malformed.
#[inline]
pub fn split<const N: usize>(line: &'static [u8], out: &mut [Bs; N]) -> bool {
    if line.is_empty() {
        return false;
    }
    let quoted = memchr::memchr(b'"', line).is_some();
    let mut start = 0usize;
    let mut f = 0usize;
    let bytes = line;
    let mut i = 0usize;
    let n = bytes.len();
    while i < n {
        if bytes[i] == b',' {
            if f + 1 >= N {
                return false;
            }
            let s = &line[start..i];
            out[f] = if quoted {
                match unquote(s) {
                    Some(b) => b,
                    None => return false,
                }
            } else {
                Bs::new(s)
            };
            f += 1;
            start = i + 1;
        }
        i += 1;
    }
    if f + 1 != N {
        return false;
    }
    let s = &line[start..];
    out[f] = if quoted {
        match unquote(s) {
            Some(b) => b,
            None => return false,
        }
    } else {
        Bs::new(s)
    };
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check<const N: usize>(chunk: &'static [u8]) {
        let mut want = Vec::new();
        for_lines(chunk, |line| {
            let mut fields = [Bs::EMPTY; N];
            if split(line, &mut fields) {
                want.push(fields.map(Bs::get));
            }
        });
        let mut got = Vec::new();
        for_records::<N>(chunk, |fields| got.push(fields.map(Bs::get)));
        assert_eq!(got, want, "dispatched scanner on {chunk:?}");

        got.clear();
        for_records_masked::<N, false>(chunk, |fields| got.push(fields.map(Bs::get)));
        assert_eq!(got, want, "baseline scanner on {chunk:?}");

        #[cfg(target_arch = "x86_64")]
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("popcnt") {
            got.clear();
            unsafe { check_baseline_avx2(chunk, |fields| got.push(fields.map(Bs::get))) };
            assert_eq!(got, want, "AVX2 baseline scanner on {chunk:?}");
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2,popcnt")]
    unsafe fn check_baseline_avx2<const N: usize>(chunk: &'static [u8], f: impl FnMut(&[Bs; N])) {
        for_records_masked::<N, true>(chunk, f);
    }

    /// The SIMD scanners must agree with line-by-line `split` on arbitrary byte soup.
    #[test]
    fn scanner_matches_split() {
        let alphabet = b"ab,,\"\"\n\r 9";
        let mut seed: u64 = 0x1234_5678;
        for round in 0..3000 {
            let len = (round % 300) + 1;
            let mut buf = Vec::with_capacity(len);
            for _ in 0..len {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                buf.push(alphabet[(seed >> 33) as usize % alphabet.len()]);
            }
            let chunk: &'static [u8] = Box::leak(buf.into_boxed_slice());
            check::<2>(chunk);
            check::<4>(chunk);
            check::<7>(chunk);
        }
    }

    #[test]
    fn scanner_matches_split_across_blocks() {
        let long = "x".repeat(513);
        let cases = [
            format!(
                concat!(
                    ",,\r,\n",
                    "\"\",\" u1\",c_1,1791071890000\r\n",
                    "bad,\",c,t\n",
                    "bad,\"u\"\r,c,t\n",
                    "bad,\"u\"\",c,t\n",
                    "bad,\"u,v\",c,t\n",
                    "bad,,,,,extra,{0}\n",
                    "short,{0}\n",
                    "long,\"{0}\",\r,\"end\"\n",
                    "bad,{0}\"{0},c,t\n",
                    "eof,user,\"c_1\",\r"
                ),
                long
            ),
            format!(
                concat!(
                    ",,,,,,\n",
                    "e,\" u1\",2026-10-03T23:58:10Z,2026-10-03T23:58:10Z,view,\"\",\r\n",
                    "bad,u,t,i,view,\",9\n",
                    "bad,u,t,i,view,\"p\"\r,9\n",
                    "bad,u,t,i,view,\"p\"\",9\n",
                    "bad,u,t,i,view,\"p,q\",9\n",
                    "bad,,,,,,,,{0}\n",
                    "short,{0}\n",
                    "long,u,t,i,view,\"{0}\",\r\n",
                    "bad,u,t,i,view,{0}\"{0},9\n",
                    "eof,u,t,i,view,{0},\"\""
                ),
                long
            ),
        ];
        for (index, rows) in cases.iter().enumerate() {
            for pad in 0..64 {
                for ending in ["", "\n"] {
                    let mut buf = vec![b'\n'; pad];
                    buf.extend_from_slice(rows.as_bytes());
                    buf.extend_from_slice(ending.as_bytes());
                    let chunk = Box::leak(buf.into_boxed_slice());
                    if index == 0 {
                        check::<4>(chunk);
                    } else {
                        check::<7>(chunk);
                    }
                }
            }
        }
    }
}
