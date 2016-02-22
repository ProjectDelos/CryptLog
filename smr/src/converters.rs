use encryptors::{MetaEncryptor, Addable, Int};

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

#[cfg(test)]
mod test {
    use super::{ConvertersLib, AddableConverter};

    #[test]
    fn create_addable_converter() {
        let _ = AddableConverter::new(ConvertersLib::i32_from_addable(),
                                      ConvertersLib::addable_from_i32());

    }
}
