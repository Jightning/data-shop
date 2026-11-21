//! Zero-copy byte slices, a fast string hash, and an open-addressing map keyed by byte strings.

/// A byte slice into a leaked mmap (or other 'static memory), 16 bytes, Copy, no lifetime.
#[derive(Clone, Copy)]
pub struct Bs {
    pub p: *const u8,
    pub n: u32,
}
unsafe impl Send for Bs {}
unsafe impl Sync for Bs {}

impl Bs {
    pub const EMPTY: Bs = Bs { p: std::ptr::null(), n: 0 };
    #[inline(always)]
    pub fn new(s: &[u8]) -> Bs {
        Bs { p: s.as_ptr(), n: s.len() as u32 }
    }
    #[inline(always)]
    pub fn get(self) -> &'static [u8] {
        if self.n == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.p, self.n as usize) }
        }
    }
    #[inline(always)]
    pub fn is_empty(self) -> bool {
        self.n == 0
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
        let mut buf = [0u8; 8];
        buf[..r.len()].copy_from_slice(r);
        h = mix(h, u64::from_le_bytes(buf));
    }
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
