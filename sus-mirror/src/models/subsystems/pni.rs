use super::{GroupInfo, SubsystemInfo};

pub const PNI: SubsystemInfo = SubsystemInfo {
    code: "PNI",
    name: "Sistema de Informações do Programa Nacional de Imunizações",
    description: Some("Base de dados do Programa Nacional de Imunizações (SI-PNI)."),
    long_description: Some(
        "O SI-PNI é um sistema desenvolvido para possibilitar aos gestores envolvidos no Programa \
Nacional de Imunização a avaliação dinâmica do risco quanto à ocorrência de surtos ou epidemias, \
a partir do registro dos imunobiológicos aplicados e do quantitativo populacional vacinado, \
agregados por faixa etária, período de tempo e área geográfica. Possibilita também o controle \
do estoque de imunobiológicos necessário aos administradores que têm a incumbência de programar \
sua aquisição e distribuição. Controla as indicações de aplicação de vacinas de imunobiológicos \
especiais e seus eventos adversos, dentro dos Centros de Referências em imunobiológicos especiais.",
    ),
    url: Some(
        "https://datasus.saude.gov.br/acesso-a-informacao/morbidade-hospitalar-do-sus-sih-sus/",
    ), // 🟢 main source URL
    groups: &[
        GroupInfo {
            code: "CPNI",
            name: "Cobertura Vacinal",
            description: Some(
                "Indicadores de cobertura vacinal por faixa etária e área geográfica.",
            ),
            long_description: None,
            url: Some(
                "https://datasus.saude.gov.br/acesso-a-informacao/producao-hospitalar-sih-sus/",
            ), // 🟢 second URL as reference
        },
        GroupInfo {
            code: "DPNI",
            name: "Doses Aplicadas",
            description: Some("Registros de doses aplicadas por imunobiológico."),
            long_description: None,
            url: Some(
                "https://datasus.saude.gov.br/acesso-a-informacao/producao-hospitalar-sih-sus/",
            ), // 🟢 same as above
        },
    ],
};
