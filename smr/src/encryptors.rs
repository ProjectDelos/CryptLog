extern crate rpaillier;
extern crate rand;
extern crate rustc_serialize;

pub use ramp::int::Int;
use rpaillier::{KeyPair, KeyPairBuilder};
use std::ops::Add;
use std::str::FromStr;
use rand::{OsRng, Rng};
use std::iter::repeat;
use rustc_serialize::{Encodable, Decodable, Encoder, Decoder};
use openssl::crypto::symm::{self, encrypt, decrypt};
use openssl::crypto::hash;

#[derive(Debug, Clone)]
pub struct Addable {
    pub i: Int, // integer
    m: Int, // modulus
}

impl Encodable for Addable {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        let ienc = self.i.to_str_radix(36, true);
        try!(ienc.encode(s));
        let menc = self.m.to_str_radix(36, true);
        try!(menc.encode(s));
        return Ok(());
    }
}

impl Decodable for Addable {
    fn decode<D: Decoder>(d: &mut D) -> Result<Addable, D::Error> {
        let ienc = try!(String::decode(d));
        let i = Int::from_str_radix(&ienc, 36).unwrap();
        let menc = try!(String::decode(d));
        let m = Int::from_str_radix(&menc, 36).unwrap();
        return Ok(Addable { i: i, m: m });
    }
}


impl Addable {
    fn new(i: Int, m: Int) -> Addable {
        Addable { i: i, m: m }
    }
}

impl Add for Addable {
    type Output = Addable;

    fn add(self, _rhs: Addable) -> Addable {
        assert_eq!(self.m, _rhs.m);
        return Addable::new((self.i * _rhs.i) % &self.m, self.m);
    }
}

pub struct AddEncryptor {
    key_pair: KeyPair,
}

impl AddEncryptor {
    fn new() -> AddEncryptor {
        AddEncryptor { key_pair: KeyPairBuilder::new().bits(128).finalize() }
    }
    fn encrypt(&self, i: &Int) -> Addable {
        let pk = &self.key_pair.public_key;
        return Addable::new(pk.encrypt(i), pk.n_squared.clone());
    }
    fn decrypt<R: FromStr>(&self, i: Addable) -> Result<R, R::Err> {
        let p = self.key_pair.decrypt(&i.i);
        let s = p.to_str_radix(10, false);
        return s.parse::<R>();
    }
}

#[derive(RustcEncodable, RustcDecodable, Clone, Debug)]
pub struct Encrypted {
    data: Vec<u8>,
}

#[derive(RustcEncodable, RustcDecodable, Clone, Debug)]
pub struct Eqable {
    hash: Vec<u8>,
    encrypted: Encrypted,
}

impl PartialEq for Eqable {
    fn eq(&self, other: &Eqable) -> bool {
        self.hash == other.hash
    }
}

impl Eq for Eqable {}

impl Eqable {
    pub fn new(d: &[u8], enc: Encrypted) -> Eqable {
        let h = hash::hash(hash::Type::SHA256, d);
        Eqable {
            hash: h,
            encrypted: enc,
        }
    }
}

pub struct EqEncryptor {
    encryptor: Encryptor,
}

impl EqEncryptor {
    pub fn new(enc: Encryptor) -> EqEncryptor {
        EqEncryptor { encryptor: enc }
    }
    pub fn encrypt(&self, d: &[u8]) -> Eqable {
        let e = self.encryptor.encrypt(d);
        Eqable::new(d, e)
    }
    pub fn decrypt(&self, d: Eqable) -> Vec<u8> {
        self.encryptor.decrypt(d.encrypted)
    }
}

pub struct Encryptor {
    key: Vec<u8>,
    nonce: Vec<u8>,
}

impl Encryptor {
    pub fn new() -> Encryptor {
        let mut gen = OsRng::new().expect("Failed to get OS random generator");
        let mut key: Vec<u8> = repeat(0u8).take(32).collect();
        gen.fill_bytes(&mut key[..]);
        let mut nonce: Vec<u8> = repeat(0u8).take(32).collect();
        gen.fill_bytes(&mut nonce[..]);
        Encryptor {
            key: key,
            nonce: nonce,
        }
    }
    pub fn from_key_nonce(key: Vec<u8>, nonce: Vec<u8>) -> Encryptor {
        Encryptor {
            key: key,
            nonce: nonce,
        }
    }
    pub fn encrypt(&self, s: &[u8]) -> Encrypted {
        let key = &self.key;
        let nonce = &self.nonce;
        let output = encrypt(symm::Type::AES_256_CBC, key, nonce, s);
        Encrypted { data: output }
    }

    pub fn decrypt(&self, e: Encrypted) -> Vec<u8> {
        let key = &self.key;
        let nonce = &self.nonce;
        let output = decrypt(symm::Type::AES_256_CBC, key, nonce, &e.data);
        output
    }
}

#[cfg(test)]
mod test {
    use super::{AddEncryptor, Encryptor, EqEncryptor, Int};
    #[test]
    fn additive_encryption() {
        let e = AddEncryptor::new();
        let x1 = e.encrypt(&Int::from(10));
        let x2 = e.encrypt(&Int::from(20));
        let r: i64 = e.decrypt(x1 + x2).expect("No parse error should occur.");
        assert_eq!(r, 30);
    }
    #[test]
    fn det_encryption() {
        let e = EqEncryptor::new(Encryptor::new());
        let x1 = e.encrypt("abcd".as_bytes());
        let x2 = e.encrypt("abcd".as_bytes());
        let x3 = e.encrypt("abcde".as_bytes());
        assert_eq!(x1, x2);
        assert!(x1 != x3);
        assert_eq!("abcd".as_bytes(), e.decrypt(x1).as_slice());
    }
}
