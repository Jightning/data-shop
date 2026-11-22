//! Zero-copy byte slices, a fast string hash, and an open-addressing map keyed by byte strings.

/// A byte slice into a process-lifetime mapping or read buffer.
#[derive(Clone, Copy)]
pub struct Bs(&'static [u8]);

impl Bs {
    pub const EMPTY: Bs = Bs(&[]);
    #[inline(always)]
    pub fn new(s: &'static [u8]) -> Bs {
        Bs(s)
    }
    #[inline(always)]
    pub fn get(self) -> &'static [u8] {
        self.0
    }
    #[inline(always)]
    pub fn is_empty(self) -> bool {
        self.0.is_empty()
    }
}

#[inline(always)]
fn mix(h: u64, v: u64) -> u64 {
    let x = (h ^ v).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^ (x >> 31)
}

/// Fast, well-mixed 64-bit hash of a short byte string. Equality is always re-checked on bytes,
/// so collisions only cost time.
#[inline]
pub fn hash(b: &[u8]) -> u64 {
    let mut h: u64 = (b.len() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x2545_F491_4F6C_DD1D;
    let mut chunks = b.chunks_exact(8);
    for c in &mut chunks {
        h = mix(h, u64::from_le_bytes(c.try_into().unwrap()));
    }
    let r = chunks.remainder();
    if !r.is_empty() {
        h = mix(h, read_partial(r)); // overlapping loads, no memcpy call
    }
    final_mix(h)
}

#[inline(always)]
fn final_mix(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

#[derive(Clone, Copy)]
struct Slot<V: Copy> {
    h: u64,
    k: Bs,
    v: V,
    used: bool,
}

/// Open-addressing (linear probing) map from byte strings to small Copy values.
/// The caller supplies the hash so it is computed once per row.
pub struct StrMap<V: Copy + Default> {
    slots: Vec<Slot<V>>,
    mask: usize,
    pub len: usize,
}

impl<V: Copy + Default> StrMap<V> {
    pub fn with_capacity(n: usize) -> Self {
        let cap = (n.max(8) * 2).next_power_of_two();
        StrMap {
            slots: vec![Slot { h: 0, k: Bs::EMPTY, v: V::default(), used: false }; cap],
            mask: cap - 1,
            len: 0,
        }
    }

    #[inline]
    pub fn get(&self, h: u64, k: &[u8]) -> Option<V> {
        if self.len == 0 {
            return None;
        }
        let mut i = (h as usize) & self.mask;
        loop {
            let s = &self.slots[i];
            if !s.used {
                return None;
            }
            if s.h == h && s.k.get() == k {
                return Some(s.v);
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Returns the existing value, or inserts `v` and returns (v, true).
    #[inline]
    pub fn get_or_insert(&mut self, h: u64, k: Bs, v: V) -> (V, bool) {
        if (self.len + 1) * 2 > self.slots.len() {
            self.grow();
        }
        let kb = k.get();
        let mut i = (h as usize) & self.mask;
        loop {
            let s = &mut self.slots[i];
            if !s.used {
                *s = Slot { h, k, v, used: true };
                self.len += 1;
                return (v, true);
            }
            if s.h == h && s.k.get() == kb {
                return (s.v, false);
            }
            i = (i + 1) & self.mask;
        }
    }

    fn grow(&mut self) {
        let cap = self.slots.len() * 2;
        let old = std::mem::replace(
            &mut self.slots,
            vec![Slot { h: 0, k: Bs::EMPTY, v: V::default(), used: false }; cap],
        );
        self.mask = cap - 1;
        for s in old.into_iter().filter(|s| s.used) {
            let mut i = (s.h as usize) & self.mask;
            while self.slots[i].used {
                i = (i + 1) & self.mask;
            }
            self.slots[i] = s;
        }
    }
}

enum CatalogRows {
    Dense(Vec<u32>),
    General(StrMap<u32>),
}

/// Use a dense index only when every catalog ID has the exact decimal spelling
/// and its index fits the bounded table derived from the number of catalog rows.
pub struct Catalog<const FIRST: u8, const SECOND: u8 = 0> {
    rows: CatalogRows,
}

impl<const FIRST: u8, const SECOND: u8> Catalog<FIRST, SECOND> {
    const MISSING: u32 = u32::MAX;

    pub fn new(map: StrMap<u32>) -> Self {
        let limit = map.len.saturating_mul(2).min(1 << 20);
        let mut rows = vec![Self::MISSING; limit + 1];
        let mut used = 0;
        for slot in map.slots.iter().filter(|slot| slot.used) {
            let Some(index) = Self::index(slot.k.get()).filter(|&i| i <= limit) else {
                return Self { rows: CatalogRows::General(map) };
            };
            rows[index] = slot.v;
            used = used.max(index + 1);
        }
        rows.truncate(used);
        Self { rows: CatalogRows::Dense(rows) }
    }

    #[inline(always)]
    fn index(key: &[u8]) -> Option<usize> {
        let digits = if SECOND == 0 {
            key.strip_prefix(&[FIRST])?
        } else {
            key.strip_prefix(&[FIRST, SECOND])?
        };
        let (&first, rest) = digits.split_first()?;
        if digits.len() > 7 || (first == b'0' && !rest.is_empty()) {
            return None;
        }
        let mut value = 0;
        for &byte in digits {
            let digit = byte.wrapping_sub(b'0');
            if digit > 9 {
                return None;
            }
            value = value * 10 + usize::from(digit);
        }
        Some(value)
    }

    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<u32> {
        match &self.rows {
            CatalogRows::Dense(rows) => {
                let value = *rows.get(Self::index(key)?)?;
                (value != Self::MISSING).then_some(value)
            }
            CatalogRows::General(map) => map.get(hash(key), key),
        }
    }
}

// ------------------------------------------------------------------ inline keys

/// Reads 0..=8 bytes as a little-endian u64 without touching memory outside `s`.
#[inline(always)]
fn read_partial(s: &[u8]) -> u64 {
    let n = s.len();
    if n >= 4 {
        let a = u32::from_le_bytes(s[..4].try_into().unwrap()) as u64;
        let b = u32::from_le_bytes(s[n - 4..].try_into().unwrap()) as u64;
        a | (b << ((n - 4) * 8))
    } else if n > 0 {
        (s[0] as u64) | ((s[n / 2] as u64) << (n / 2 * 8)) | ((s[n - 1] as u64) << ((n - 1) * 8))
    } else {
        0
    }
}

/// Encode a fixed-prefix ID with an implicit prefix and an inline tag bit.
#[inline(always)]
pub(crate) fn pack_prefixed(bytes: &[u8], prefix: u8) -> Option<u64> {
    let n = bytes.len();
    if n == 0 || n > 9 || bytes[0] != prefix || bytes[n - 1] == 0 {
        return None;
    }
    let tail = &bytes[1..];
    let word = if n == 9 {
        u64::from_be_bytes(tail.try_into().unwrap())
    } else {
        read_partial(tail).swap_bytes()
    };
    // The high bit must be free for the tag. A nonzero final byte makes
    // zero padding unambiguous and preserves byte-wise order.
    (word >> 63 == 0).then_some((word << 1) | 1)
}

/// Packed prefixed ID, or a pointer to a stable byte-slice descriptor.
#[derive(Clone, Copy)]
pub struct CompactKey<const PREFIX: u8>(u64);

pub type UserKey = CompactKey<b'u'>;
pub type ClickKey = CompactKey<b'k'>;

/// Thread-local cursor over fallback descriptors. Published descriptors and
/// their mapped bytes stay live until process exit. A parser that only sees
/// inline keys allocates no descriptor storage.
#[derive(Default)]
pub struct KeyArena {
    free: &'static mut [Bs],
}

impl KeyArena {
    #[cold]
    fn refill(&mut self) {
        self.free = Box::leak(Box::new([Bs::EMPTY; 512]));
    }

    fn insert(&mut self, bytes: Bs) -> u64 {
        if self.free.is_empty() {
            self.refill();
        }
        let (slot, rest) = std::mem::take(&mut self.free).split_first_mut().unwrap();
        self.free = rest;
        *slot = bytes;
        (slot as *const Bs).expose_provenance() as u64
    }
}

impl<const PREFIX: u8> CompactKey<PREFIX> {
    pub const EMPTY: Self = Self(0);

    /// Return the ID and its hash from a single byte load on the common path.
    #[inline(always)]
    pub fn hashed(id: Bs, arena: &mut KeyArena) -> (u64, Self) {
        let bytes = id.get();
        if let Some(key) = Self::inline(bytes) {
            (final_mix(key.0), key)
        } else {
            (hash(bytes), Self(arena.insert(id)))
        }
    }

    #[inline(always)]
    pub fn new(id: Bs, arena: &mut KeyArena) -> Self {
        Self::inline(id.get()).unwrap_or_else(|| Self(arena.insert(id)))
    }

    #[inline(always)]
    fn inline(bytes: &[u8]) -> Option<Self> {
        pack_prefixed(bytes, PREFIX).map(Self)
    }

    #[inline(always)]
    fn is_inline(self) -> bool {
        self.0 & 1 != 0
    }

    #[inline(always)]
    fn is_empty(self) -> bool {
        self.0 == Self::EMPTY.0
    }

    fn fallback_bytes(self) -> &'static [u8] {
        if self.is_empty() {
            return &[];
        }
        // A nonzero even key came from an aligned, initialized descriptor in
        // KeyArena. That slot is never mutated again and the chunk stays live.
        let descriptor = std::ptr::with_exposed_provenance::<Bs>(self.0 as usize);
        unsafe { (*descriptor).get() }
    }

    #[inline]
    pub fn with_bytes<R>(self, f: impl FnOnce(&[u8]) -> R) -> R {
        if self.is_inline() {
            let word = self.0 >> 1;
            let len = 8 - word.trailing_zeros() as usize / 8;
            let mut bytes = [PREFIX; 9];
            bytes[1..].copy_from_slice(&word.to_be_bytes());
            f(&bytes[..len + 1])
        } else {
            f(self.fallback_bytes())
        }
    }

    #[inline]
    pub fn same(self, other: Self) -> bool {
        self.0 == other.0
            || (!self.is_inline() && !other.is_inline() && self.fallback_bytes() == other.fallback_bytes())
    }

    #[inline]
    pub fn cmp(self, other: Self) -> std::cmp::Ordering {
        if self.is_inline() && other.is_inline() {
            self.0.cmp(&other.0)
        } else {
            self.with_bytes(|a| other.with_bytes(|b| a.cmp(b)))
        }
    }
}

#[derive(Clone, Copy)]
struct KSlot<V: Copy> {
    h: u32,
    k: UserKey,
    v: V,
}

/// Open-addressing map keyed by `UserKey` (hash supplied by the caller).
pub struct KeyMap<V: Copy + Default> {
    slots: Vec<KSlot<V>>,
    mask: usize,
    pub len: usize,
}

impl<V: Copy + Default> KeyMap<V> {
    pub fn with_capacity(n: usize) -> Self {
        let cap = (n.max(8) * 2).next_power_of_two();
        KeyMap {
            slots: vec![KSlot { h: 0, k: UserKey::EMPTY, v: V::default() }; cap],
            mask: cap - 1,
            len: 0,
        }
    }

    #[inline]
    pub fn get(&self, h: u32, k: &UserKey) -> Option<V> {
        if self.len == 0 {
            return None;
        }
        let mut i = (h as usize) & self.mask;
        loop {
            let s = &self.slots[i];
            if s.k.is_empty() {
                return None;
            }
            if s.h == h && s.k.same(*k) {
                return Some(s.v);
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline]
    pub fn get_or_insert(&mut self, h: u32, k: UserKey, v: V) -> (V, bool) {
        if (self.len + 1) * 2 > self.slots.len() {
            self.grow();
        }
        let mut i = (h as usize) & self.mask;
        loop {
            let s = &mut self.slots[i];
            if s.k.is_empty() {
                *s = KSlot { h, k, v };
                self.len += 1;
                return (v, true);
            }
            if s.h == h && s.k.same(k) {
                return (s.v, false);
            }
            i = (i + 1) & self.mask;
        }
    }

    fn grow(&mut self) {
        let cap = self.slots.len() * 2;
        let old = std::mem::replace(
            &mut self.slots,
            vec![KSlot { h: 0, k: UserKey::EMPTY, v: V::default() }; cap],
        );
        self.mask = cap - 1;
        for s in old.into_iter().filter(|s| !s.k.is_empty()) {
            let mut i = (s.h as usize) & self.mask;
            while !self.slots[i].k.is_empty() {
                i = (i + 1) & self.mask;
            }
            self.slots[i] = s;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_keys_match_byte_slices() {
        assert_eq!(std::mem::size_of::<UserKey>(), 8);
        assert_eq!(std::mem::size_of::<KSlot<u32>>(), 16);
        let samples: &[&'static [u8]] = &[
            b"", b"a", b"u", b"ua", b"uaa", b"uaaa", b"uaaaa", b"uaaaaa", b"uaaaaaa",
            b"uaaaaaaa", b"uaaaaaaaa", b"uaaaaaaaaa", b"u0", b"u0 ", b"uA", b"U0",
            b" u0", b"u0\r", b"u0\0", b"u0\0b", b"u0\0\0", b"u\0b", b"u\0",
            "ué".as_bytes(), "uaé".as_bytes(), "u你".as_bytes(), b"verylongendoffile",
        ];
        let mut arena = KeyArena::default();
        for &a in samples {
            let (ha, ka) = UserKey::hashed(Bs::new(a), &mut arena);
            ka.with_bytes(|bytes| assert_eq!(bytes, a));
            for &b in samples {
                let (hb, kb) = UserKey::hashed(Bs::new(b), &mut arena);
                assert_eq!(ka.same(kb), a == b, "{a:?} {b:?}");
                assert_eq!(ka.cmp(kb), a.cmp(b), "{a:?} {b:?}");
                if a == b {
                    assert_eq!(ha, hb);
                }
            }
        }
        let click = ClickKey::new(Bs::new(b"k01234567"), &mut arena);
        click.with_bytes(|bytes| assert_eq!(bytes, b"k01234567"));
    }

    #[test]
    fn fallback_keys_survive_arena_refill_and_drop() {
        let key = {
            let mut arena = KeyArena::default();
            let first = UserKey::new(Bs::new(b"Ueof"), &mut arena);
            for _ in 0..1025 {
                UserKey::new(Bs::new(b"ua00000000000000000"), &mut arena);
            }
            first
        };
        key.with_bytes(|bytes| assert_eq!(bytes, b"Ueof"));
    }

    #[test]
    fn key_map_keeps_hash_collisions_and_grows() {
        let mut arena = KeyArena::default();
        let mut map = KeyMap::with_capacity(0);
        for index in 0..512 {
            let bytes = Box::leak(format!("u{index}").into_bytes().into_boxed_slice());
            let key = UserKey::new(Bs::new(bytes), &mut arena);
            assert_eq!(map.get_or_insert(1, key, index), (index, true));
            assert_eq!(map.get(1, &key), Some(index));
        }
        let fallback = UserKey::new(Bs::new(b"u1\0"), &mut arena);
        assert_eq!(map.get_or_insert(1, fallback, 512), (512, true));
        let fallback2 = UserKey::new(Bs::new(b"u1\0"), &mut arena);
        assert_eq!(map.get(1, &fallback2), Some(512));
    }
}
