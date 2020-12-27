use std::convert::{From, TryFrom};

use rosc::{OscMessage, OscType};
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum MessageError {
    #[error("Invalid address `{0}`")]
    InvalidAddr(String),
    #[error("Invalid arguments")]
    InvalidArgs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
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

impl TryFrom<OscMessage> for Request {
    type Error = anyhow::Error;

    fn try_from(msg: OscMessage) -> anyhow::Result<Request> {
        let OscMessage { addr, args } = msg;
        let args = args.into_iter()
                       .map(|x| x.int().ok_or(MessageError::InvalidArgs))
                       .collect::<Result<Vec<_>, _>>()?;
        let get = |n: usize| args.get(n).map(|x| *x).ok_or(MessageError::InvalidArgs);
        match addr.as_str() {
            "/InitZero" => Ok(Request::InitZero(get(0)?, get(1)?)),
            "/X" => Ok(Request::X(get(0)?, get(1)?)),
            "/Y" => Ok(Request::Y(get(0)?, get(1)?)),
            "/Z" => Ok(Request::Z(get(0)?, get(1)?)),
            "/H" => Ok(Request::H(get(0)?, get(1)?)),
            "/S" => Ok(Request::S(get(0)?, get(1)?)),
            "/Sdg" => Ok(Request::Sdg(get(0)?, get(1)?)),
            "/T" => Ok(Request::T(get(0)?, get(1)?)),
            "/Tdg" => Ok(Request::Tdg(get(0)?, get(1)?)),
            "/CX" => Ok(Request::CX(get(0)?, get(1)?, get(2)?, get(3)?)),
            "/Mz" => Ok(Request::Mz(get(0)?, get(1)?)),
            _ => Err(MessageError::InvalidAddr(addr).into())
        }
    }
}

impl From<&Request> for OscMessage {
    fn from(msg: &Request) -> OscMessage {
        match msg {
            Request::InitZero(n1, n2) => OscMessage { addr: "/InitZero".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Request::X(n1, n2) => OscMessage { addr: "/X".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Request::Y(n1, n2) => OscMessage { addr: "/Y".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Request::Z(n1, n2) => OscMessage { addr: "/Z".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Request::H(n1, n2) => OscMessage { addr: "/H".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Request::S(n1, n2) => OscMessage { addr: "/S".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Request::Sdg(n1, n2) => OscMessage { addr: "/Sdg".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Request::T(n1, n2) => OscMessage { addr: "/T".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Request::Tdg(n1, n2) => OscMessage { addr: "/Tdg".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
            Request::CX(n1, n2, n3, n4) => OscMessage { addr: "/CX".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2), OscType::Int(*n3), OscType::Int(*n4)] },
            Request::Mz(n1, n2) => OscMessage { addr: "/Mz".to_owned(), args: vec![OscType::Int(*n1), OscType::Int(*n2)] },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    Mz(i32, f32)
}

impl TryFrom<OscMessage> for Response {
    type Error = anyhow::Error;

    fn try_from(msg: OscMessage) -> anyhow::Result<Response> {
        let OscMessage { addr, args } = msg;
        match addr.as_str() {
            "/Mz" => Ok(Response::Mz(args.get(0).and_then(|x| x.clone().int()).ok_or(MessageError::InvalidArgs)?,
                                     args.get(1).and_then(|x| x.clone().float()).ok_or(MessageError::InvalidArgs)?)),
            _ => Err(MessageError::InvalidAddr(addr).into())
        }
    }
}

impl From<&Response> for OscMessage {
    fn from(msg: &Response) -> OscMessage {
        match msg {
            Response::Mz(n1, f1) => OscMessage { addr: "/Mz".to_owned(), args: vec![OscType::Int(*n1), OscType::Float(*f1)] },
        }
    }
}
