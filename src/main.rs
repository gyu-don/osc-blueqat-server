use std::convert::TryFrom;
use std::env::args;
use std::net::SocketAddr;
use std::str::FromStr;

use tokio::task;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::signal::ctrl_c;

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

/// Loop for sending response to client.
async fn sender_loop(tx_addr: SocketAddr, mut chan_rx: mpsc::Receiver<Response>) -> anyhow::Result<()> {
    //let tx = UdpSocket::bind(tx_addr).await?;
    let tx = std::net::UdpSocket::bind("0.0.0.0:9999")?;
    while let Some(msg) = chan_rx.recv().await {
        info!("sender_loop: Received from channel: {:?}", msg);
        /*
        let packet = rosc::encoder::encode(&OscPacket::Bundle(
                OscBundle { timetag: (0, 0),
                            content: vec![OscPacket::Message(OscMessage::from(&msg))]
                })).map_err(|e| anyhow!("{:?}", e))?;
        */
        let packet = rosc::encoder::encode(&OscPacket::Message(OscMessage::from(&msg)))
            .map_err(|e| anyhow!("{:?}", e))?;
        info!("sender_loop: Encoded packet (len={}): {:?}", packet.len(), packet);
        info!("sender_loop: Sending to {}...", tx_addr);
        //tx.send(&packet).await?;
        tx.send_to(&packet, tx_addr)?;
        info!("sender_loop: Sent.");
    }
    bail!("sender_loop: unexpected finished");
}

/// Loop for receiving request from client.
async fn receiver_loop(host_rx_addr: SocketAddr, chan_tx: mpsc::Sender<Request>) -> anyhow::Result<()> {
    let mut buf = vec![0; OSC_BUF_LEN];
    let rx = UdpSocket::bind(host_rx_addr).await?;
    loop {
        info!("receiver_loop: Receiving from {}...", host_rx_addr);
        let len = rx.recv(&mut buf).await?;
        info!("receiver_loop: Received. len={}, bytes={:?}", len, &buf[..len]);
        let packet = rosc::decoder::decode(&buf[..len]);
        let packet = match packet {
            Ok(inner) => inner,
            Err(e) => {
                warn!("receiver_loop: OSC Error {:?}", e);
                continue;
            }
        };
        info!("receiver_loop: OSC Message: {:?}", packet);
        let msg = Request::try_from(match packet {
            OscPacket::Message(msg) => {
                warn!("receiver_loop: Message without Bundle");
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
        info!("receiver_loop: Message: {:?}", msg);
        chan_tx.send(msg).await?;
    }
}

async fn runner_loop(mut ops_rx: mpsc::Receiver<Request>, result_tx: mpsc::Sender<Response>) -> anyhow::Result<()> {
    let mut sim = BlueqatSimulator::new().unwrap();
    let mut ops = BlueqatOperations::new();
    let mut result = String::new();
    ops.initialize();
    info!("runner_loop: Start");
    while let Some(msg) = ops_rx.recv().await {
        info!("runner_loop: Message received from channel. {:?}", msg);
        match msg {
            Request::X(_, n) => ops.x(n as Qubit),
            Request::Y(_, n) => ops.y(n as Qubit),
            Request::Z(_, n) => ops.z(n as Qubit),
            Request::H(_, n) => ops.h(n as Qubit),
            Request::CX(_, n1, _, n2) => ops.cx(n1 as Qubit, n2 as Qubit),
            Request::Mz(_, n) => {
                info!("runner_loop: Received Mz inst.");
                ops.measure(n as Qubit, ());
                info!("runner_loop: Calling blueqat...");
                sim.send_receive(&ops, &mut result).await;
                info!("runner_loop: Blueqat response: {}", result);
                let bit = (result.as_bytes()[n as usize] - b'0') as i32;
                result_tx.send(Response::Mz(bit, 0.0)).await?;
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
    let sender = task::spawn(sender_loop(tx, result_rx));
    let runner = task::spawn(runner_loop(ops_rx, result_tx));
    let receiver = task::spawn(receiver_loop(rx, ops_tx));

    ctrl_c().await?;
    receiver.abort();
    runner.abort();
    sender.abort();
    Ok(())
}
