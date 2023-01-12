pub enum TXMessageKind {
    DEFAULT,
    COMMITTED,
    ERROR,
    TERMINATE,
    ENDOFRAMPUP,
}

pub struct TXMessage {
    pub kind: TXMessageKind,
    pub tx_id: u16,
    pub client_id: u32,
    pub tx_duration_us: u128,
    pub tx_timestamp: i64,
    pub error: String,
}

impl TXMessage {
    pub fn default() -> TXMessage {
        TXMessage {
            kind: TXMessageKind::DEFAULT,
            tx_id: 0,
            client_id: 0,
            tx_duration_us: 0,
            tx_timestamp: 0,
            error: "".to_string(),
        }
    }

    pub fn terminate_data_collector() -> TXMessage {
        let mut m = Self::default();
        m.kind = TXMessageKind::TERMINATE;

        m
    }

    pub fn error(tx_id: u16, client_id: u32, tx_timestamp: i64, error: String) -> TXMessage {
        let mut m = Self::default();
        m.kind = TXMessageKind::ERROR;
        m.tx_id = tx_id;
        m.client_id = client_id;
        m.tx_timestamp = tx_timestamp;
        m.error = error;

        m
    }

    pub fn committed(tx_id: u16, client_id: u32, tx_timestamp: i64, tx_duration_us: u128) -> TXMessage {
        let mut m = Self::default();
        m.kind = TXMessageKind::COMMITTED;
        m.tx_id = tx_id;
        m.client_id = client_id;
        m.tx_timestamp = tx_timestamp;
        m.tx_duration_us = tx_duration_us;

        m
    }

    pub fn end_of_rampup() -> TXMessage {
        let mut m = Self::default();
        m.kind = TXMessageKind::ENDOFRAMPUP;

        m
    }
}
