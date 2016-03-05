extern crate rand;
extern crate ramp;

use std::cmp::Ordering;
use std::marker::PhantomData;
use self::rand::{SeedableRng, IsaacRng};
use self::ramp::{Int, RandomInt};

use encryptors::RingInt;

pub trait BitTraversable {
    fn next_bit(&mut self) -> bool;
    fn bit_len(&mut self) -> usize;
}

impl BitTraversable for Vecu8Traversable {
    fn next_bit(&mut self) -> bool {
        // caller's responsibility to not go overbounds
        assert_eq!(self.bt.len() == 0, false);

        if self.count > 0 {
            let mut x = self.bt.pop().unwrap();
            self.count -= 1;
            let res = (x & (1 << self.count) >= 1) as bool;
            if res {
                x -= 1 << self.count;
            }

            self.bt.push(x);
            res
        } else {
            self.bt.pop();
            self.count = 8;
            self.next_bit()
        }
    }

    fn bit_len(&mut self) -> usize {
        self.bt.len() * 8
    }
}

#[derive(Clone)]
pub struct Vecu8Traversable {
    pub bt: Vec<u8>,
    count: u8,
}
impl Vecu8Traversable {
    pub fn new(bt: &[u8]) -> Vecu8Traversable {
        let mut bt = Vec::from(bt);
        bt.reverse(); // bytes must be compared starting with most significant
        return Vecu8Traversable { bt: bt, count: 8 };
    }
}

pub trait PRNG<S> {
    // expands randomness by a factor of 2
    fn next(&self, s: S) -> (S, S);
}

// random function created from pseudo random number generator P
// and individually applied to each bit in T
#[derive(Clone)]
pub struct RandomFn<P: PRNG<Int>, T: BitTraversable> {
    prng: P,
    key: Int,
    m: Int,
    pd: PhantomData<T>,
}

// #[derive(Clone, Debug)]
// pub struct OrdInt {
//    i: Int,
//    m: Int,
// }

pub type OrdInt = RingInt;
// impl OrdInt {
// pub fn new(i: Int, m: Int) -> OrdInt {
// OrdInt { i: i, m: m }
// }
// }
//

impl PartialEq for OrdInt {
    fn eq(&self, other: &OrdInt) -> bool {
        self.i == other.i
    }
}

impl Eq for OrdInt {}

impl PartialOrd for OrdInt {
    fn partial_cmp(&self, other: &OrdInt) -> Option<Ordering> {
        if self.eq(other) {
            return Some(Ordering::Equal);
        }
        let (_, ip1) = (&self.i + Int::one()).divmod(&self.m);
        if ip1 == other.i {
            return Some(Ordering::Less);
        }
        Some(Ordering::Greater)
    }
}

impl Ord for OrdInt {
    fn cmp(&self, other: &OrdInt) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub type OrdData = Vec<OrdInt>;

impl<P: PRNG<Int>, BT: BitTraversable + Clone> RandomFn<P, BT> {
    pub fn new(prng: P, key: Int, m: Int) -> RandomFn<P, BT> {
        return RandomFn {
            prng: prng,
            key: key,
            m: m,
            pd: PhantomData,
        };
    }

    // Random function called on every bit in v, to get order revealing encryption
    // let u1..un be the encryption for bits b1..bn of v, where n = bit_len
    // ui = (F(key, b0..bi-1) + bi ) mod m
    // Random function is obtained by repeatedly calling a PRNG we call G
    // F(key, b0..bi-1) = G(G(G(key)[b0])[b1]...)[bi-1]
    // where g[bi] mean: the first half of g if bi is 0, the second half of g otherwise
    pub fn on(&self, v: BT) -> OrdData {
        let mut v = v.clone();
        let mut s = self.key.clone();
        let mut res = Vec::new();

        let mut prev_bit = false;
        let bl = v.bit_len();
        for _ in 0..bl {
            let cur_bit = v.next_bit();
            let cur_bit_int = match cur_bit {
                true => Int::one(),
                false => Int::zero(),
            };

            // choice between g0 and g1 has to be made based on previous bit
            // to allow for easy comparison
            let (g0, g1) = self.prng.next(s.clone());
            if prev_bit == false {
                let g0 = g0 + cur_bit_int;
                let (_, smod) = g0.divmod(&self.m);
                res.push(OrdInt::new(smod, self.m.clone()));
                s = g0;
            } else {
                let g1 = g1 + cur_bit_int;
                let (_, smod) = g1.divmod(&self.m);
                res.push(OrdInt::new(smod, self.m.clone()));
                s = g1;
            }
            prev_bit = cur_bit;
        }
        res
    }
}


#[derive(Clone)]
pub struct RandomIntPRNG;

impl PRNG<Int> for RandomIntPRNG {
    fn next(&self, s: Int) -> (Int, Int) {
        let mut s = s;

        // seed as 4 integers
        let mut seed: Vec<u32> = Vec::new();
        let p32 = Int::from(2).pow(32);
        let mask = &p32 - Int::one();
        for _ in 0..4 {
            let bint = &s & &mask;
            seed.push(u32::from(&bint));
            s = (&s) / (&p32);
        }

        let mut rng: IsaacRng = SeedableRng::from_seed(seed.as_ref());
        let g0 = rng.gen_int(128);
        let g1 = rng.gen_int(128);
        (g0, g1)
    }
}

#[cfg(test)]
mod test {
    extern crate ramp;
    extern crate rand;
    use self::rand::{SeedableRng, IsaacRng};
    use self::ramp::{Int, RandomInt};
    use std::collections::BTreeMap;

    use super::{PRNG, RandomIntPRNG, RandomFn, BitTraversable, Vecu8Traversable};

    #[test]
    fn use_next_bit() {
        let mut s0 = Vecu8Traversable::new(&String::from("h0").into_bytes());
        let mut s1 = Vecu8Traversable::new(&String::from("h1").into_bytes());

        // s0 and s1 differ in only last of the 16 bits
        for _ in 0..8 {
            assert_eq!(s0.next_bit(), s1.next_bit())
        }
        for _ in 0..7 {
            assert_eq!(s0.next_bit(), s1.next_bit())
        }
        assert_eq!(s0.next_bit(), false);
        assert_eq!(s1.next_bit(), true);
    }

    #[test]
    fn use_random_intgen() {
        let mut rng: IsaacRng = SeedableRng::from_seed(vec![1, 2, 3, 4].as_ref());
        let seed = rng.gen_int(128);

        let gen = RandomIntPRNG;

        let (g0, g1) = gen.next(seed);
        let (x0, x1) = gen.next(g0);
        let (y0, y1) = gen.next(g1);

        assert_eq!(x0 == x1, false);
        assert_eq!(y0 == y1, false);
        assert_eq!(x0 == y0, false);
    }

    #[test]
    fn use_random_fn() {
        // number generator to help us deterministcly choose a key
        let mut rng: IsaacRng = SeedableRng::from_seed(vec![1, 2, 3, 4].as_ref());
        let key = rng.gen_int(128);

        let m = Int::from(2).pow(40);

        let rf: RandomFn<RandomIntPRNG, Vecu8Traversable> = RandomFn::new(RandomIntPRNG, key, m);
        let messgs = vec![("h1", String::from("h1").into_bytes()),
                          ("h2", String::from("h2").into_bytes()),
                          ("h0", String::from("h0").into_bytes()),
                          ("alphabet", String::from("alphabet").into_bytes()),
                          ("h0rry", String::from("h0rry").into_bytes())];

        let mut res = Vec::new();
        for i in 0..messgs.len() {
            let messg = messgs[i].clone();
            res.push((messg.0, rf.on(Vecu8Traversable::new(&messg.1))));
        }

        // sort by orderable encryption
        res.sort_by(|a, b| a.1.cmp(&b.1));
        // check sort by unencrypted key
        // it should be alphabet, h0, h0rry, h1, h2
        for i in 0..res.len() - 1 {
            assert_eq!(res[i].0 < res[i + 1].0, true);
        }

        // for r in &res {
        //    println!("res = {:?}", r);
        // }
    }

    #[test]
    fn btreemap_encrypted_keys() {
        // number generator to help us deterministcly choose a key
        let mut rng: IsaacRng = SeedableRng::from_seed(vec![1, 2, 3, 4].as_ref());
        let key = rng.gen_int(128);

        let m = Int::from(2).pow(40);

        let rf: RandomFn<RandomIntPRNG, Vecu8Traversable> = RandomFn::new(RandomIntPRNG, key, m);
        let keys = vec!["h0", "h1", "h2", "alpha", "h0rry"];
        let vals = vec![2, 4, 5, 1, 3];

        let mut bmap = BTreeMap::new();
        for i in 0..keys.len() {
            // bmap.insert(keys[i].clone(), vals[i].clone());
            let kb = String::from(keys[i]).into_bytes();
            bmap.insert(rf.on(Vecu8Traversable::new(&kb)), vals[i].clone());
        }

        let mut i = 1;
        for (_, v) in bmap.iter() {
            // println!(" {}", v);
            assert_eq!(v.clone(), i);
            i += 1;
        }

    }
}
