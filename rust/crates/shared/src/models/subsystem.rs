use once_cell::sync::Lazy;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubsystemMetadata {
    pub long_name: String,
    pub source: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subsystem {
    pub name: String,
    pub metadata: SubsystemMetadata,
}

impl Subsystem {
    pub fn new(name: String, metadata: SubsystemMetadata) -> Self {
        Self { name, metadata }
    }
}

pub static SIA: Lazy<Subsystem> = Lazy::new(|| {
    Subsystem::new(
        "SIA".to_string(),
        SubsystemMetadata {
            long_name: "Sistema de Informações Ambulatoriais".to_string(),
            source: "http://sia.datasus.gov.br/principal/index.php".to_string(),
            description: [
                "O Sistema de Informação Ambulatorial (SIA) foi instituído pela ",
                "Portaria GM/MS n.º 896 de 29 de junho de 1990. Originalmente, o ",
                "SIA foi concebido a partir do projeto SICAPS (Sistema de ",
                "Informação e Controle Ambulatorial da Previdência Social), em ",
                "que os conceitos, os objetivos e as diretrizes criados para o ",
                "desenvolvimento do SICAPS foram extremamente importantes e ",
                "amplamente utilizados para o desenvolvimento do SIA, tais",
                " como: (i) o acompanhamento das programações físicas e ",
                "orçamentárias; (ii) o acompanhamento das ações de saúde ",
                "produzidas; (iii) a agilização do pagamento e controle ",
                "orçamentário e financeiro; e (iv) a formação de banco de dados ",
                "para contribuir com a construção do SUS."
            ].join(""),
        },
    )
});

pub static SIH: Lazy<Subsystem> = Lazy::new(|| {
    Subsystem::new(
        "SIH".to_string(),
        SubsystemMetadata {
            long_name: "Sistema de Informações Hospitalares".to_string(),
            source: [
                "https://datasus.saude.gov.br/acesso-a-informacao/morbidade-hospitalar-do-sus-sih-sus/",
                "https://datasus.saude.gov.br/acesso-a-informacao/producao-hospitalar-sih-sus/"
            ].join(" | "),
            description: [
                "A finalidade do AIH (Sistema SIHSUS) é a de transcrever todos os ",
                "atendimentos que provenientes de internações hospitalares que ",
                "foram financiadas pelo SUS, e após o processamento, gerarem ",
                "relatórios para os gestores que lhes possibilitem fazer os ",
                "pagamentos dos estabelecimentos de saúde. Além disso, o nível ",
                "Federal recebe mensalmente uma base de dados de todas as ",
                "internações autorizadas (aprovadas ou não para pagamento) para ",
                "que possam ser repassados às Secretarias de Saúde os valores de ",
                "Produção de Média e Alta complexidade além dos valores de CNRAC, ",
                "FAEC e de Hospitais Universitários – em suas variadas formas de ",
                "contrato de gestão."
            ].join(""),
        },
    )
});

pub static CIHA: Lazy<Subsystem> = Lazy::new(|| {
    Subsystem::new(
        "CIHA".to_string(),
        SubsystemMetadata {
            long_name: "Comunicação de Internação Hospitalar e Ambulatorial".to_string(),
            source: "http://ciha.datasus.gov.br/CIHA/index.php".to_string(),
            description: [
                "A CIHA foi criada para ampliar o processo de planejamento, ",
                "programação, controle, avaliação e regulação da assistência à ",
                "saúde permitindo um conhecimento mais abrangente e profundo dos ",
                "perfis nosológico e epidemiológico da população brasileira, da ",
                "capacidade instalada e do potencial de produção de serviços do ",
                "conjunto de estabelecimentos de saúde do País. O sistema permite ",
                "o acompanhamento das ações e serviços de saúde custeados ",
                "por: planos privados de assistência à saúde; planos públicos; ",
                "pagamento particular por pessoa física; pagamento particular por ",
                "pessoa jurídica; programas e projetos federais (PRONON, PRONAS, ",
                "PROADI); recursos próprios das secretarias municipais e estaduais",
                " de saúde; DPVAT; gratuidade e, a partir da publicação da ",
                "Portaria GM/MS nº 2.905/2022, consórcios públicos. As ",
                "informações registradas na CIHA servem como base para o processo ",
                "de Certificação de Entidades Beneficentes de Assistência Social ",
                "em Saúde (CEBAS) e para monitoramento dos programas PRONAS e ",
                "PRONON"
            ].join(""),
        },
    )
});

pub static CNES: Lazy<Subsystem> = Lazy::new(|| {
    Subsystem::new(
        "CNES".to_string(),
        SubsystemMetadata {
            long_name: "Cadastro Nacional de Estabelecimentos de Saúde".to_string(),
            source: "https://cnes.datasus.gov.br/".to_string(),
            description: [
                "O Cadastro Nacional de Estabelecimentos de Saúde (CNES) é o ",
                "sistema de informação oficial de cadastramento de informações ",
                "de todos os estabelecimentos de saúde no país, independentemente ",
                "de sua natureza jurídica ou de integrarem o Sistema Único de ",
                "Saúde (SUS). Trata-se do cadastro oficial do Ministério da ",
                "Saúde (MS) no tocante à realidade da capacidade instalada e ",
                "mão-de-obra assistencial de saúde no Brasil em estabelecimentos ",
                "de saúde públicos ou privados, com convênio SUS ou não."
            ].join(""),
        },
    )
});

pub static IBGE: Lazy<Subsystem> = Lazy::new(|| {
    Subsystem::new(
        "IBGE".to_string(),
        SubsystemMetadata {
            long_name: "Populaçao Residente, Censos, Contagens Populacionais e Projeçoes Intercensitarias".to_string(),
            source: "ftp://ftp.datasus.gov.br/dissemin/publicos/IBGE".to_string(),
            description: [
                "São aqui apresentados informações sobre a população residente, ",
                "estratificadas por município, faixas etárias e sexo, obtidas a ",
                "partir dos Censos Demográficos, Contagens Populacionais ",
                "e Projeções Intercensitárias."
            ].join(""),
        },
    )
});

pub static PNI: Lazy<Subsystem> = Lazy::new(|| {
    Subsystem::new(
        "PNI".to_string(),
        SubsystemMetadata {
            long_name: "Sistema de Informações do Programa Nacional de Imunizações".to_string(),
            source: [
                "https://datasus.saude.gov.br/acesso-a-informacao/morbidade-hospitalar-do-sus-sih-sus/",
                "https://datasus.saude.gov.br/acesso-a-informacao/producao-hospitalar-sih-sus/"
            ].join(" | "),
            description: [
                "O SI-PNI é um sistema desenvolvido para possibilitar aos ",
                "gestores envolvidos no Programa Nacional de Imunização, a ",
                "avaliação dinâmica do risco quanto à ocorrência de surtos ou ",
                "epidemias, a partir do registro dos imunobiológicos aplicados e ",
                "do quantitativo populacional vacinado, agregados por faixa ",
                "etária, período de tempo e área geográfica. Possibilita também ",
                "o controle do estoque de imunobiológicos necessário aos ",
                "administradores que têm a incumbência de programar sua aquisição ",
                "e distribuição. Controla as indicações de aplicação de ",
                "vacinas de imunobiológicos especiais e seus eventos adversos, ",
                "dentro dos Centros de Referências em imunobiológicos especiais."
            ].join(""),
        },
    )
});

pub static SIM: Lazy<Subsystem> = Lazy::new(|| {
    Subsystem::new(
        "SIM".to_string(),
        SubsystemMetadata {
            long_name: "Sistema de Informação sobre Mortalidade".to_string(),
            source: "http://sim.saude.gov.br".to_string(),
            description: [
                "O Sistema de Informação sobre Mortalidade (SIM) é um sistema ",
                "desenvolvido pelo Ministério da Saúde (MS) para coletar, ",
                "processar e disponibilizar informações sobre a mortalidade ",
                "no Brasil. O SIM é responsável por coletar informações sobre ",
                "a mortalidade em todo o país, incluindo dados sobre a idade, ",
                "sexo, localização e causa da morte."
            ].join(""),
        },
    )
});

pub static SINAN: Lazy<Subsystem> = Lazy::new(|| {
    Subsystem::new(
        "SINAN".to_string(),
        SubsystemMetadata {
            long_name: "Doenças e Agravos de Notificação".to_string(),
            source: "https://portalsinan.saude.gov.br/".to_string(),
            description: [
                "The Notifiable Diseases Information System - Sinan is primarily",
                "fed by the notification and investigation of cases of diseases ",
                "and conditions listed in the national list of compulsorily ",
                "notifiable diseases (Consolidation Ordinance No. 4, September 28,",
                " 2017, Annex). However, states and municipalities are allowed to ",
                "include other important health problems in their region, such as ",
                "difilobotriasis in the municipality of São Paulo. Its effective ",
                "use enables the dynamic diagnosis of the occurrence of an event ",
                "in the population, providing evidence for causal explanations of ",
                "compulsorily notifiable diseases and indicating risks to which ",
                "people are exposed. This contributes to identifying the ",
                "epidemiological reality of a specific geographical area. Its ",
                "systematic, decentralized use contributes to the democratization ",
                "of information, allowing all healthcare professionals to access ",
                "and make it available to the community. Therefore, it is a ",
                "relevant tool to assist in health planning, define intervention ",
                "priorities, and evaluate the impact of interventions."
            ].join(""),
        },
    )
});

pub static SINASC: Lazy<Subsystem> = Lazy::new(|| {
    Subsystem::new(
        "SINASC".to_string(),
        SubsystemMetadata {
            long_name: "Sistema de Informações sobre Nascidos Vivos".to_string(),
            source: "http://sinasc.saude.gov.br/".to_string(),
            description: "Dados sobre nascidos vivos no Brasil".to_string(),
        },
    )
});
