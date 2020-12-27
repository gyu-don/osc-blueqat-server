use std::convert::TryFrom;
use std::env::args;
use std::net::SocketAddr;
use std::str::FromStr;

use tokio::task;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use anyhow::{anyhow, bail, ensure};

#[allow(unused_imports)]
use log::{LevelFilter, info, warn};

use message::{Response, Request};
use rosc::{OscBundle, OscMessage, OscPacket};

use lay::{Operations, gates::CliffordGate};
use lay_simulator_blueqat::{BlueqatSimulator, BlueqatOperations};

mod message;

type Qubit = <BlueqatOperations as Operations>::Qubit;

const QUEUE_LEN: usize = 100;
const OSC_BUF_LEN: usize = 1000;

async fn sender_loop(tx_addr: SocketAddr, mut chan_rx: mpsc::Receiver<Response>) -> anyhow::Result<()> {
    let tx = UdpSocket::bind(tx_addr).await?;
    while let Some(msg) = chan_rx.recv().await {
        info!("host_sender_loop: Received from channel: {:?}", msg);
        let packet = rosc::encoder::encode(&OscPacket::Bundle(
                OscBundle { timetag: (0, 0),
                            content: vec![OscPacket::Message(OscMessage::from(&msg))]
                })).map_err(|e| anyhow!("{:?}", e))?;
        tx.send(&packet).await?;
    }
    bail!("host_sender_loop unexpected finished");
}

async fn receiver_loop(host_rx_addr: SocketAddr, chan_tx: mpsc::Sender<Request>) -> anyhow::Result<()> {
    let rx = UdpSocket::bind(host_rx_addr).await?;
    let mut buf = vec![0; OSC_BUF_LEN];
    loop {
        info!("Receiving from {}...", host_rx_addr);
        let len = rx.recv(&mut buf).await?;
        let packet = rosc::decoder::decode(&buf[..len]).map_err(|e| anyhow!("{:?}", e))?;
        let msg = Request::try_from(match packet {
            OscPacket::Message(msg) => {
                warn!("Message without Bundle");
                msg
            },
            OscPacket::Bundle(mut bundle) => {
                ensure!(bundle.content.len() != 0, "Received empty bundle.");
                ensure!(bundle.content.len() == 1, "Multiple messages in same bundle.");
                match bundle.content.pop().unwrap() {
                    OscPacket::Message(msg) => msg,
                    OscPacket::Bundle(_bundle) => bail!("Received nested bundle.")
                }
            }
        })?;
        buf.clear();
        chan_tx.send(msg).await?;
    }
}

async fn runner_loop(mut ops_rx: mpsc::Receiver<Request>, mut result_tx: mpsc::Sender<Response>) -> anyhow::Result<()> {
    let mut sim = BlueqatSimulator::new().unwrap();
    let mut ops = BlueqatOperations::new();
    let mut result = String::new();
    ops.initialize();
    while let Some(msg) = ops_rx.recv().await {
        match msg {
            Request::X(_, n) => ops.x(n as Qubit),
            Request::Y(_, n) => ops.y(n as Qubit),
            Request::Z(_, n) => ops.z(n as Qubit),
            Request::H(_, n) => ops.h(n as Qubit),
            Request::CX(_, n1, _, n2) => ops.cx(n1 as Qubit, n2 as Qubit),
            Request::Mz(_, n) => {
                ops.measure(n as Qubit, ());
                sim.send_receive(&ops, &mut result).await;
                //let bit = (result.as_bytes()[n as usize] - b'0') as u32;
                result_tx.send(Response::Mz(0, 0.0)).await?;
                ops = BlueqatOperations::new();
                result.clear();
            },
            _ => unimplemented!()
        }
    }
    bail!("runner_loop unexpected exit");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_default_env().filter_level(LevelFilter::Info).init();
    let tx = args().nth(1)
                        .and_then(|s| SocketAddr::from_str(&s).ok())
                        .expect("Sender addr required.");
    let rx = args().nth(2)
                        .and_then(|s| SocketAddr::from_str(&s).ok())
                        .expect("Receiver addr required.");
    let (ops_tx, ops_rx) = mpsc::channel(QUEUE_LEN);
    let (result_tx, result_rx) = mpsc::channel(QUEUE_LEN);
    let receiver = task::spawn(receiver_loop(rx, ops_tx));
    let runner = task::spawn(runner_loop(ops_rx, result_tx));
    let sender = task::spawn(sender_loop(tx, result_rx));

    receiver.await??;
    runner.await??;
    sender.await??;
    Ok(())
}
