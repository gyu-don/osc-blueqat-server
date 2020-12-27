use std::convert::{From, TryFrom};

use rosc::{OscMessage, OscType};
use thiserror::Error;

#[derive(Debug, Clone)]
pub enum Message {
    InitZero(i32, i32),
    X(i32, i32),
    Y(i32, i32),
    Z(i32, i32),
    H(i32, i32),
    S(i32, i32),
    Sdg(i32, i32),
    T(i32, i32),
    Tdg(i32, i32),
    CX(i32, i32, i32, i32),
    Mz(i32, i32),
}

#[derive(Debug, Clone, Error)]
pub enum MessageError {
    #[error("Invalid address `{0}`")]
    InvalidAddr(String),
    #[error("Invalid arguments")]
    InvalidArgs,
}

impl TryFrom<OscMessage> for Message {
    type Error = anyhow::Error;

    fn try_from(msg: OscMessage) -> anyhow::Result<Message> {
        let OscMessage { addr, args } = msg;
        let args = args.into_iter()
                       .map(|x| x.int().ok_or(MessageError::InvalidArgs))
                       .collect::<Result<Vec<_>, _>>()?;
        let get = |n: usize| args.get(n).map(|x| *x).ok_or(MessageError::InvalidArgs);
        match addr.as_str() {
            "/InitZero" => Ok(Message::InitZero(get(0)?, get(1)?)),
            "/X" => Ok(Message::X(get(0)?, get(1)?)),
            "/Y" => Ok(Message::Y(get(0)?, get(1)?)),
            "/Z" => Ok(Message::Z(get(0)?, get(1)?)),
            "/H" => Ok(Message::H(get(0)?, get(1)?)),
            "/S" => Ok(Message::S(get(0)?, get(1)?)),
            "/Sdg" => Ok(Message::Sdg(get(0)?, get(1)?)),
            "/T" => Ok(Message::T(get(0)?, get(1)?)),
            "/Tdg" => Ok(Message::Tdg(get(0)?, get(1)?)),
            "/CX" => Ok(Message::CX(get(0)?, get(1)?, get(2)?, get(3)?)),
            "/Mz" => Ok(Message::Mz(get(0)?, get(1)?)),
            _ => Err(MessageError::InvalidAddr(addr).into())
        }
    }
}

impl From<&Message> for OscMessage {
    fn from(msg: &Message) -> OscMessage {
        match msg {
            Message::InitZero(n1, n2) => OscMessage { addr: "/InitZero".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Message::X(n1, n2) => OscMessage { addr: "/X".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Message::Y(n1, n2) => OscMessage { addr: "/Y".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Message::Z(n1, n2) => OscMessage { addr: "/Z".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Message::H(n1, n2) => OscMessage { addr: "/H".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Message::S(n1, n2) => OscMessage { addr: "/S".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Message::Sdg(n1, n2) => OscMessage { addr: "/Sdg".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Message::T(n1, n2) => OscMessage { addr: "/T".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Message::Tdg(n1, n2) => OscMessage { addr: "/Tdg".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Message::CX(n1, n2, n3, n4) => OscMessage { addr: "/CX".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2), OscType::Int(*n3), OscType::Int(*n4)] },
            Message::Mz(n1, n2) => OscMessage { addr: "/Mz".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
        }
    }
}
