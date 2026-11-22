//! Timestamp forms F1/F2/F3 and the price field, exactly per rules.md (ASCII digits only).

pub const DAY: i64 = 86_400_000;

#[inline(always)]
fn d1(c: u8) -> Option<u32> {
    let v = c.wrapping_sub(b'0');
    if v < 10 {
        Some(v as u32)
    } else {
        None
    }
}

#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<u32> {
    Some(d1(b[i])? * 10 + d1(b[i + 1])?)
}

const fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's algorithm).
const fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

const fn year_starts() -> [i32; 10_000] {
    let mut years = [0; 10_000];
    let mut year = 0;
    while year < years.len() {
        let y = year as i64;
        // The low bit selects the leap-year month table.
        years[year] = (days_from_civil(y, 1, 1) as i32) * 2 + is_leap(y) as i32;
        year += 1;
    }
    years
}

static YEAR_STARTS: [i32; 10_000] = year_starts();

const fn months() -> [[(i32, u32); 13]; 2] {
    let mut months = [[(0, 0); 13]; 2];
    let mut leap = 0;
    while leap < 2 {
        let year = 2001 - leap as i64;
        let jan = days_from_civil(year, 1, 1);
        let mut month = 1;
        while month <= 12 {
            let start = days_from_civil(year, month as u32, 1);
            let end = if month == 12 {
                days_from_civil(year + 1, 1, 1)
            } else {
                days_from_civil(year, month as u32 + 1, 1)
            };
            months[leap][month] = ((start - jan) as i32, (end - start) as u32);
            month += 1;
        }
        leap += 1;
    }
    months
}

const MONTHS: [[(i32, u32); 13]; 2] = months();

const ASCII_ZEROS: u64 = u64::from_le_bytes(*b"00000000");
const HIGH_NIBBLES: u64 = 0xf0f0_f0f0_f0f0_f0f0;
const LOW_NIBBLES: u64 = !HIGH_NIBBLES;

/// A nonzero result marks bytes outside ASCII '0' through '9' in their high nibble.
#[inline(always)]
fn non_digits(word: u64) -> u64 {
    // A low nibble of 10 or more carries into the high nibble when adding 6.
    let carry = ((word & LOW_NIBBLES) + 0x0606_0606_0606_0606) & HIGH_NIBBLES;
    ((word & HIGH_NIBBLES) ^ ASCII_ZEROS) | carry
}

#[inline(always)]
fn decimal_pair(word: u64) -> u32 {
    ((word & 0xf) * 10 + ((word >> 8) & 0xf)) as u32
}

/// Decode and validate the shared F1/F2 prefix.
#[inline(always)]
fn parse_datetime(b: &[u8; 19]) -> Option<i64> {
    let date = u64::from_le_bytes(b[..8].try_into().unwrap());
    let time = u64::from_le_bytes(b[8..16].try_into().unwrap());
    let second = u16::from_le_bytes([b[17], b[18]]) as u64;
    const DATE_DIGITS: u64 = u64::from_le_bytes([255, 255, 255, 255, 0, 255, 255, 0]);
    const TIME_DIGITS: u64 = u64::from_le_bytes([255, 255, 0, 255, 255, 0, 255, 255]);
    const DATE_SEPARATORS: u64 = u64::from_le_bytes(*b"\0\0\0\0-\0\0-");
    const TIME_SEPARATORS: u64 = u64::from_le_bytes(*b"\0\0T\0\0:\0\0");
    let bad_separators = ((date ^ DATE_SEPARATORS) & !DATE_DIGITS)
        | ((time ^ TIME_SEPARATORS) & !TIME_DIGITS)
        | u64::from(b[16] ^ b':');
    let bad_digits = (non_digits(date) & DATE_DIGITS)
        | (non_digits(time) & TIME_DIGITS)
        | (non_digits(second) & 0xffff);
    if bad_separators | bad_digits != 0 {
        return None;
    }
    let y = decimal_pair(date) * 100 + decimal_pair(date >> 16);
    let mo = decimal_pair(date >> 40);
    let d = decimal_pair(time);
    let h = decimal_pair(time >> 24);
    let mi = decimal_pair(time >> 48);
    let s = decimal_pair(second);

    let year = *YEAR_STARTS.get(y as usize)?;
    let (month_start, month_len) = *MONTHS[(year & 1) as usize].get(mo as usize)?;
    let day_in_month = d.wrapping_sub(1);
    if day_in_month >= month_len || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let days = (year >> 1) as i64 + i64::from(month_start) + i64::from(day_in_month);
    let seconds = (h * 60 + mi) * 60 + s;
    Some(days * DAY + i64::from(seconds) * 1000)
}

/// Checked F1 parser.
#[inline]
pub fn parse_ingest(b: &[u8]) -> Option<i64> {
    if let [prefix @ .., b'Z'] = <&[u8; 20]>::try_from(b).ok()? {
        parse_datetime(prefix)
    } else {
        None
    }
}

#[inline(always)]
fn decimal_pairs(word: u64) -> u64 {
    let digits = word & LOW_NIBBLES;
    (digits.wrapping_mul(10).wrapping_add(digits >> 8)) & 0x00ff_00ff_00ff_00ff
}

/// Decimal conversion in parallel pairs, with input order from the least significant byte.
#[inline(always)]
fn decimal8(word: u64) -> u64 {
    let pairs = decimal_pairs(word);
    let groups = (pairs.wrapping_mul(100).wrapping_add(pairs >> 16)) & 0x0000_ffff_0000_ffff;
    (groups.wrapping_mul(10_000).wrapping_add(groups >> 32)) & 0xffff_ffff
}

#[inline(always)]
fn parse_f3_13(b: &[u8; 13]) -> Option<i64> {
    let first = u64::from_le_bytes(b[..8].try_into().unwrap());
    // Overlap the loads to keep the last five digits in a single word.
    let last = u64::from_le_bytes(b[5..].try_into().unwrap());
    if non_digits(first) | non_digits(last) != 0 {
        return None;
    }
    let pairs = decimal_pairs(last);
    let last_four = ((pairs >> 32) & 0xff) * 100 + (pairs >> 48);
    let last_five = ((last >> 24) & 0xf) * 10_000 + last_four;
    Some((decimal8(first) * 100_000 + last_five) as i64)
}

/// Parses F1, F2 or F3 to epoch milliseconds (UTC). An F3 value too large for
/// i64 can never be inside a window.
#[inline]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    match b.len() {
        20 if b[19] == b'Z' => return parse_ingest(b),
        25 if b[19] == b'+' || b[19] == b'-' => {
            if b[22] != b':' {
                return None;
            }
            let oh = d2(b, 20)?;
            let om = d2(b, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let local = parse_datetime(b.first_chunk()?)?;
            let offset = (oh as i64 * 3600 + om as i64 * 60) * 1000;
            return Some(if b[19] == b'+' {
                local - offset
            } else {
                local + offset
            });
        }
        13 => return parse_f3_13(b.try_into().unwrap()),
        _ => {}
    }

    if b.is_empty() {
        return None;
    }
    let mut value = 0i64;
    for &c in b {
        value = value.checked_mul(10)?.checked_add(d1(c)? as i64)?;
    }
    Some(value)
}

/// Purchase price: ASCII digits only, value 1..=10,000,000 (leading zeros allowed).
#[inline]
pub fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            // Still must check the remaining bytes are digits? No: any value over the limit is
            // invalid whatever follows, and a non-digit is invalid too. Either way: None.
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn forms() {
        let f1 = parse_ts(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse_ts(b"1791071890000"), Some(1_791_071_890_000));
        assert_eq!(parse_ts(b"0001791071890000"), Some(1_791_071_890_000));
        assert_eq!(f1, 1_791_071_890_000);
        assert_eq!(
            parse_ts(b"2026-10-04T01:30:00+02:00"),
            parse_ts(b"2026-10-03T23:30:00Z")
        );
        for bad in [
            &b"2026-10-03 10:00:00"[..],
            b"2026-10-03T10:00:00z",
            b"2026-02-29T00:00:00Z",
            b"2026-10-03T24:00:00Z",
            b"2026-10-03T10:00:60Z",
            b"2026-10-03T10:00:00+24:00",
            b"2026-10-03T10:00:00+05:60",
            b"2026-10-03T10:00:00+0200",
            b"1791071890000.0",
            b"-5",
            b"1.79e12",
            b"2026-10-03T10:00:00.000Z",
            b"",
            b"2026-13-01T00:00:00Z",
            b"99999999999999999999999",
        ] {
            assert_eq!(parse_ts(bad), None, "{}", String::from_utf8_lossy(bad));
        }
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(
            parse_ts(b"2026-10-03T10:00:00-00:00"),
            parse_ts(b"2026-10-03T10:00:00Z")
        );
        assert_eq!(parse_price(b"0250"), Some(250));
        assert_eq!(parse_price(b"10000000"), Some(10_000_000));
        assert_eq!(parse_price(b"10000001"), None);
        assert_eq!(parse_price(b"0"), None);
        assert_eq!(parse_price(b"19.99"), None);
        assert_eq!(parse_price(b"1000\r"), None);
    }
    #[test]
    fn f1_gregorian_month_boundaries_match_scalar_calendar() {
        let mut day = -719_528; // 0000-01-01, relative to 1970-01-01.
        for year in 0..10_000 {
            let february = if year % 400 == 0 || (year % 4 == 0 && year % 100 != 0) {
                29
            } else {
                28
            };
            for (i, length) in [31, february, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
                .into_iter()
                .enumerate()
            {
                let month = i + 1;
                let first = format!("{year:04}-{month:02}-01T00:00:00Z");
                let last = format!("{year:04}-{month:02}-{length:02}T23:59:59Z");
                let invalid = format!("{year:04}-{month:02}-{:02}T00:00:00Z", length + 1);
                for (ts, expected) in [(first, day * DAY), (last, (day + length) * DAY - 1000)] {
                    assert_eq!(parse_ts(ts.as_bytes()), Some(expected), "{ts}");
                }
                assert_eq!(parse_ts(invalid.as_bytes()), None, "{invalid}");
                day += length;
            }
        }
    }

    #[test]
    fn f1_rejects_every_non_ascii_digit_and_wrong_separator() {
        let sample = b"2028-02-29T12:34:56Z";
        for (i, original) in sample.iter().copied().enumerate() {
            for byte in 0..=u8::MAX {
                if byte == original || (original.is_ascii_digit() && byte.is_ascii_digit()) {
                    continue;
                }
                let mut changed = *sample;
                changed[i] = byte;
                assert_eq!(parse_ts(&changed), None, "position {i}, byte {byte}");
                assert_eq!(parse_ingest(&changed), None, "position {i}, byte {byte}");
            }
        }
        assert_eq!(parse_ingest(&sample[..19]), None);
    }

    #[test]
    fn f2_offsets_cross_days_and_years() {
        for (local, utc) in [
            ("2028-03-01T00:03:00+00:04", "2028-02-29T23:59:00Z"),
            ("2027-12-31T23:59:59-23:59", "2028-01-01T23:58:59Z"),
            ("2028-01-01T00:00:00+23:59", "2027-12-31T00:01:00Z"),
        ] {
            assert_eq!(
                parse_ts(local.as_bytes()),
                parse_ts(utc.as_bytes()),
                "{local}"
            );
        }
        assert_eq!(
            parse_ts(b"0000-01-01T00:00:00+23:59"),
            Some(-719_529 * DAY + 60_000)
        );
        assert_eq!(
            parse_ts(b"9999-12-31T23:59:59-00:01"),
            Some(253_402_300_859_000)
        );
        assert_eq!(parse_ts(b"1900-02-29T00:00:00-00:00"), None);
    }

    #[test]
    fn f3_fast_and_general_paths_match_scalar_digits() {
        fn scalar(b: &[u8]) -> Option<i64> {
            if b.is_empty() || !b.iter().all(u8::is_ascii_digit) {
                return None;
            }
            std::str::from_utf8(b).ok()?.parse().ok()
        }

        let mut state = 0u64;
        for _ in 0..4096 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let value = format!("{:013}", state % 10_000_000_000_000);
            assert_eq!(
                parse_ts(value.as_bytes()),
                scalar(value.as_bytes()),
                "{value}"
            );
        }
        let sample = b"1791071890123";
        for i in 0..sample.len() {
            for byte in 0..=u8::MAX {
                let mut changed = *sample;
                changed[i] = byte;
                assert_eq!(
                    parse_ts(&changed),
                    scalar(&changed),
                    "position {i}, byte {byte}"
                );
            }
        }

        for value in [
            "0",
            "000",
            "9999999999999",
            "9223372036854775807",
            "9223372036854775808",
            "9x",
        ] {
            let long = format!("{:0>1300}", value);
            for ts in [value.as_bytes(), long.as_bytes()] {
                assert_eq!(parse_ts(ts), scalar(ts));
            }
        }
        // F3 also reaches the fallback at the lengths used by F1 and F2.
        assert_eq!(parse_ts(b"00000000000000000123"), Some(123));
        assert_eq!(parse_ts(b"0000000000000000000000123"), Some(123));
    }
}
