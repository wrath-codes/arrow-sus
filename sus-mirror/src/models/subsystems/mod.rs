use serde::Serialize;
pub mod cih;
pub mod ciha;
pub mod cnes;
pub mod ibge;
pub mod pni;
pub mod resp;
pub mod sia;
pub mod sih;
pub mod sim;
pub mod sinan;
pub mod sinasc;
pub mod siscolo;
pub mod sismama;
pub mod sisprenatal;

pub use cih::CIH;
pub use ciha::CIHA;
pub use cnes::CNES;
pub use ibge::IBGE;
pub use pni::PNI;
pub use resp::RESP;
pub use sia::SIASUS;
pub use sih::SIHSUS;
pub use sim::SIM;
pub use sinan::SINAN;
pub use sinasc::SINASC;
pub use siscolo::SISCOLO;
pub use sismama::SISMAMA;
pub use sisprenatal::SISPRENATAL;

#[derive(Debug, Serialize)]
pub struct SubsystemInfo {
    pub code: &'static str,
    pub name: &'static str,
    pub description: Option<&'static str>,
    pub long_description: Option<&'static str>,
    pub url: Option<&'static str>,
    pub groups: &'static [GroupInfo],
}

#[derive(Debug, Serialize)]
pub struct GroupInfo {
    pub code: &'static str,
    pub name: &'static str,
    pub description: Option<&'static str>,
    pub long_description: Option<&'static str>,
    pub url: Option<&'static str>,
}

pub const SUBSYSTEMS: &[SubsystemInfo] = &[
    CIH,
    CIHA,
    CNES,
    IBGE,
    PNI,
    RESP,
    SIASUS,
    SIHSUS,
    SIM,
    SINAN,
    SINASC,
    SISCOLO,
    SISMAMA,
    SISPRENATAL,
];
