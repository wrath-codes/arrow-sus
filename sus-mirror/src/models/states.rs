use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct StateInfo {
    pub uf: &'static str,        // e.g. "SP"
    pub name: &'static str,      // e.g. "São Paulo"
    pub ibge_code: &'static str, // e.g. "35"
}

pub const STATES: &[StateInfo] = &[
    StateInfo {
        uf: "AC",
        name: "Acre",
        ibge_code: "12",
    },
    StateInfo {
        uf: "AL",
        name: "Alagoas",
        ibge_code: "27",
    },
    StateInfo {
        uf: "AP",
        name: "Amapá",
        ibge_code: "16",
    },
    StateInfo {
        uf: "AM",
        name: "Amazonas",
        ibge_code: "13",
    },
    StateInfo {
        uf: "BA",
        name: "Bahia",
        ibge_code: "29",
    },
    StateInfo {
        uf: "CE",
        name: "Ceará",
        ibge_code: "23",
    },
    StateInfo {
        uf: "DF",
        name: "Distrito Federal",
        ibge_code: "53",
    },
    StateInfo {
        uf: "ES",
        name: "Espírito Santo",
        ibge_code: "32",
    },
    StateInfo {
        uf: "GO",
        name: "Goiás",
        ibge_code: "52",
    },
    StateInfo {
        uf: "MA",
        name: "Maranhão",
        ibge_code: "21",
    },
    StateInfo {
        uf: "MT",
        name: "Mato Grosso",
        ibge_code: "51",
    },
    StateInfo {
        uf: "MS",
        name: "Mato Grosso do Sul",
        ibge_code: "50",
    },
    StateInfo {
        uf: "MG",
        name: "Minas Gerais",
        ibge_code: "31",
    },
    StateInfo {
        uf: "PA",
        name: "Pará",
        ibge_code: "15",
    },
    StateInfo {
        uf: "PB",
        name: "Paraíba",
        ibge_code: "25",
    },
    StateInfo {
        uf: "PR",
        name: "Paraná",
        ibge_code: "41",
    },
    StateInfo {
        uf: "PE",
        name: "Pernambuco",
        ibge_code: "26",
    },
    StateInfo {
        uf: "PI",
        name: "Piauí",
        ibge_code: "22",
    },
    StateInfo {
        uf: "RJ",
        name: "Rio de Janeiro",
        ibge_code: "33",
    },
    StateInfo {
        uf: "RN",
        name: "Rio Grande do Norte",
        ibge_code: "24",
    },
    StateInfo {
        uf: "RS",
        name: "Rio Grande do Sul",
        ibge_code: "43",
    },
    StateInfo {
        uf: "RO",
        name: "Rondônia",
        ibge_code: "11",
    },
    StateInfo {
        uf: "RR",
        name: "Roraima",
        ibge_code: "14",
    },
    StateInfo {
        uf: "SC",
        name: "Santa Catarina",
        ibge_code: "42",
    },
    StateInfo {
        uf: "SP",
        name: "São Paulo",
        ibge_code: "35",
    },
    StateInfo {
        uf: "SE",
        name: "Sergipe",
        ibge_code: "28",
    },
    StateInfo {
        uf: "TO",
        name: "Tocantins",
        ibge_code: "17",
    },
];
