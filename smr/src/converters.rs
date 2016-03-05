extern crate rustc_serialize;
use self::rustc_serialize::json;
use self::rustc_serialize::{Encodable, Decodable};
use encryptors::{MetaEncryptor, Addable, Eqable, Ordable, Encrypted, Int};

use std::sync::Arc;

pub struct ConvertersLib;

impl ConvertersLib {
    fn m_addable_from_addable(secure: &Option<MetaEncryptor>, a: Addable) -> Addable {
        return a;
    }

    fn m_addable_from_i32(secure: &Option<MetaEncryptor>, val: i32) -> Addable {
        secure.as_ref()
              .map(|secure| secure.encrypt_ahe(Int::from(val)))
              .unwrap()
    }

    fn m_i32_from_addable(secure: &Option<MetaEncryptor>, a: Addable) -> i32 {
        match secure {
            &Some(ref secure) => {
                match secure.decrypt_ahe::<i32>(a) {
                    Ok(data) => data,
                    Err(e) => panic!("error decrypting {}", e), 
                }
            }
            &None => panic!("no secure given"),
        }
    }

    fn m_ordable_from_ordable(secure: &Option<MetaEncryptor>, e: Ordable) -> Ordable {
        e
    }

    fn m_ordable_from_encodable<E: Encodable + Decodable>(secure: &Option<MetaEncryptor>,
                                                          s: E)
                                                          -> Ordable {
        let data = json::encode(&s).unwrap();
        secure.as_ref()
              .map(|secure| secure.encrypt_ordable(&data.into_bytes()))
              .unwrap()
    }

    fn m_encodable_from_ordable<E: Encodable + Decodable>(secure: &Option<MetaEncryptor>,
                                                          e: Ordable)
                                                          -> E {
        match secure {
            &Some(ref secure) => {
                let data = String::from_utf8(secure.decrypt_ordable(e)).unwrap();
                json::decode(&data).unwrap()
            }
            &None => panic!("no secure given"),
        }

    }

    fn m_eqable_from_eqable(secure: &Option<MetaEncryptor>, e: Eqable) -> Eqable {
        e
    }

    fn m_eqable_from_encodable<E: Encodable + Decodable>(secure: &Option<MetaEncryptor>,
                                                         s: E)
                                                         -> Eqable {
        let data = json::encode(&s).unwrap();
        secure.as_ref()
              .map(|secure| secure.encrypt_eqable(&data.into_bytes()))
              .unwrap()
    }

    fn m_encodable_from_eqable<E: Encodable + Decodable>(secure: &Option<MetaEncryptor>,
                                                         e: Eqable)
                                                         -> E {
        match secure {
            &Some(ref secure) => {
                let data = String::from_utf8(secure.decrypt_eqable(e)).unwrap();
                json::decode(&data).unwrap()
            }
            &None => panic!("no secure given"),
        }

    }

    fn m_encrypted_from_encrypted(secure: &Option<MetaEncryptor>, e: Encrypted) -> Encrypted {
        e
    }

    fn m_encrypted_from_encodable<E: Encodable + Decodable>(secure: &Option<MetaEncryptor>,
                                                            s: E)
                                                            -> Encrypted {
        let data = json::encode(&s).unwrap();
        secure.as_ref()
              .map(|secure| secure.encrypt(&data.into_bytes()))
              .unwrap()
    }

    fn m_encodable_from_encrypted<E: Encodable + Decodable>(secure: &Option<MetaEncryptor>,
                                                            e: Encrypted)
                                                            -> E {
        match secure {
            &Some(ref secure) => {
                let data = String::from_utf8(secure.decrypt(e)).unwrap();
                json::decode(&data).unwrap()
            }
            &None => panic!("no secure given"),
        }
    }

    pub fn addable_from_addable
        ()
        -> Box<Fn(&Option<MetaEncryptor>, Addable) -> Addable + Send + Sync>
    {
        Box::new(ConvertersLib::m_addable_from_addable)
    }

    pub fn addable_from_i32() -> Box<Fn(&Option<MetaEncryptor>, i32) -> Addable + Send + Sync> {
        Box::new(ConvertersLib::m_addable_from_i32)
    }

    pub fn i32_from_addable() -> Box<Fn(&Option<MetaEncryptor>, Addable) -> i32 + Send + Sync> {
        Box::new(ConvertersLib::m_i32_from_addable)
    }

    pub fn ordable_from_ordable
        ()
        -> Box<Fn(&Option<MetaEncryptor>, Ordable) -> Ordable + Send + Sync>
    {
        Box::new(ConvertersLib::m_ordable_from_ordable)
    }

    pub fn ordable_from_encodable<E: 'static + Encodable + Decodable>
        ()
        -> Box<Fn(&Option<MetaEncryptor>, E) -> Ordable + Send + Sync>
    {
        Box::new(ConvertersLib::m_ordable_from_encodable)
    }

    pub fn encodable_from_ordable<E: 'static + Encodable + Decodable>
        ()
        -> Box<Fn(&Option<MetaEncryptor>, Ordable) -> E + Send + Sync>
    {
        Box::new(ConvertersLib::m_encodable_from_ordable)
    }

    pub fn eqable_from_eqable
        ()
        -> Box<Fn(&Option<MetaEncryptor>, Eqable) -> Eqable + Send + Sync>
    {
        Box::new(ConvertersLib::m_eqable_from_eqable)
    }

    pub fn eqable_from_encodable<E: 'static + Encodable + Decodable>
        ()
        -> Box<Fn(&Option<MetaEncryptor>, E) -> Eqable + Send + Sync>
    {
        Box::new(ConvertersLib::m_eqable_from_encodable)
    }

    pub fn encodable_from_eqable<E: 'static + Encodable + Decodable>
        ()
        -> Box<Fn(&Option<MetaEncryptor>, Eqable) -> E + Send + Sync>
    {
        Box::new(ConvertersLib::m_encodable_from_eqable)
    }

    pub fn encrypted_from_encrypted
        ()
        -> Box<Fn(&Option<MetaEncryptor>, Encrypted) -> Encrypted + Send + Sync>
    {
        Box::new(ConvertersLib::m_encrypted_from_encrypted)
    }

    pub fn encrypted_from_encodable<E: 'static + Encodable + Decodable>
        ()
        -> Box<Fn(&Option<MetaEncryptor>, E) -> Encrypted + Send + Sync>
    {
        Box::new(ConvertersLib::m_encrypted_from_encodable)
    }

    pub fn encodable_from_encrypted<E: 'static + Encodable + Decodable>
        ()
        -> Box<Fn(&Option<MetaEncryptor>, Encrypted) -> E + Send + Sync>
    {
        Box::new(ConvertersLib::m_encodable_from_encrypted)
    }
}

#[derive(Clone)]
pub struct AddableConverter<I> {
    pub from: Arc<Box<Fn(&Option<MetaEncryptor>, Addable) -> I + Send + Sync>>,
    pub to: Arc<Box<Fn(&Option<MetaEncryptor>, I) -> Addable + Send + Sync>>,
}

impl<I> AddableConverter<I> {
    pub fn new(from: Box<Fn(&Option<MetaEncryptor>, Addable) -> I + Send + Sync>,
               to: Box<Fn(&Option<MetaEncryptor>, I) -> Addable + Send + Sync>)
               -> AddableConverter<I> {
        AddableConverter {
            from: Arc::new(from),
            to: Arc::new(to),
        }
    }
}

#[derive(Clone)]
pub struct EqableConverter<T> {
    pub from: Arc<Box<Fn(&Option<MetaEncryptor>, Eqable) -> T + Send + Sync>>,
    pub to: Arc<Box<Fn(&Option<MetaEncryptor>, T) -> Eqable + Send + Sync>>,
}

impl<T> EqableConverter<T> {
    pub fn new(from: Box<Fn(&Option<MetaEncryptor>, Eqable) -> T + Send + Sync>,
               to: Box<Fn(&Option<MetaEncryptor>, T) -> Eqable + Send + Sync>)
               -> EqableConverter<T> {
        EqableConverter {
            from: Arc::new(from),
            to: Arc::new(to),
        }
    }
}

#[derive(Clone)]
pub struct OrdableConverter<T> {
    pub from: Arc<Box<Fn(&Option<MetaEncryptor>, Ordable) -> T + Send + Sync>>,
    pub to: Arc<Box<Fn(&Option<MetaEncryptor>, T) -> Ordable + Send + Sync>>,
}

impl<T> OrdableConverter<T> {
    pub fn new(from: Box<Fn(&Option<MetaEncryptor>, Ordable) -> T + Send + Sync>,
               to: Box<Fn(&Option<MetaEncryptor>, T) -> Ordable + Send + Sync>)
               -> OrdableConverter<T> {
        OrdableConverter {
            from: Arc::new(from),
            to: Arc::new(to),
        }
    }
}

#[derive(Clone)]
pub struct Converter<T> {
    pub from: Arc<Box<Fn(&Option<MetaEncryptor>, Encrypted) -> T + Send + Sync>>,
    pub to: Arc<Box<Fn(&Option<MetaEncryptor>, T) -> Encrypted + Send + Sync>>,
}

impl<T> Converter<T> {
    pub fn new(from: Box<Fn(&Option<MetaEncryptor>, Encrypted) -> T + Send + Sync>,
               to: Box<Fn(&Option<MetaEncryptor>, T) -> Encrypted + Send + Sync>)
               -> Converter<T> {
        Converter {
            from: Arc::new(from),
            to: Arc::new(to),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{ConvertersLib, AddableConverter, EqableConverter, OrdableConverter, Converter};

    #[test]
    fn create_addable_converter() {
        let _ = AddableConverter::new(ConvertersLib::i32_from_addable(),
                                      ConvertersLib::addable_from_i32());

    }

    #[test]
    fn create_eqable_converter() {
        let _: EqableConverter<String> =
            EqableConverter::new(ConvertersLib::encodable_from_eqable(),
                                 ConvertersLib::eqable_from_encodable());

    }

    #[test]
    fn create_ordable_converter() {
        let _: OrdableConverter<String> =
            OrdableConverter::new(ConvertersLib::encodable_from_ordable(),
                                  ConvertersLib::ordable_from_encodable());

    }

    #[test]
    fn create_converter() {
        let _: Converter<String> = Converter::new(ConvertersLib::encodable_from_encrypted(),
                                                  ConvertersLib::encrypted_from_encodable());

    }
}
