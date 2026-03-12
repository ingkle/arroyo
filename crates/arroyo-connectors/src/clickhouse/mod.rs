mod sink;

use anyhow::{anyhow, bail};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use typify::import_types;

use crate::clickhouse::sink::ClickHouseSinkFunc;
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::ConstructedOperator;

const PROFILE_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");

import_types!(
    schema = "src/clickhouse/profile.json",
    convert = { {type = "string", format = "var-str"} = VarStr }
);

import_types!(
    schema = "src/clickhouse/table.json"
);

const ICON: &str = include_str!("./clickhouse.svg");

const DEFAULT_PORT: u16 = 8123;
const DEFAULT_BATCH_SIZE: u32 = 10_000;
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 1_000;

pub struct ClickHouseConnector {}

impl ClickHouseConnector {
    fn build_base_url(config: &ClickHouseConfig) -> anyhow::Result<String> {
        let scheme = if config.https.unwrap_or(false) {
            "https"
        } else {
            "http"
        };
        let host = config.host.sub_env_vars()?;
        let port = config.port.unwrap_or(DEFAULT_PORT as i64);

        Ok(format!("{scheme}://{host}:{port}"))
    }

    fn build_insert_url(
        base_url: &str,
        config: &ClickHouseConfig,
        table: &ClickHouseTable,
    ) -> anyhow::Result<String> {
        let database = config
            .database
            .as_ref()
            .map(|d| d.sub_env_vars())
            .transpose()?
            .unwrap_or_else(|| "default".to_string());

        let query = format!(
            "INSERT INTO {}.{} FORMAT JSONEachRow",
            database, table.table_name
        );

        let mut url = format!("{}/?query={}", base_url, urlencoding::encode(&query));

        if let Some(ref user) = config.username {
            url.push_str(&format!("&user={}", urlencoding::encode(&user.sub_env_vars()?)));
        }
        if let Some(ref pass) = config.password {
            url.push_str(&format!(
                "&password={}",
                urlencoding::encode(&pass.sub_env_vars()?)
            ));
        }

        Ok(url)
    }

    async fn test_connection(
        config: &ClickHouseConfig,
        table: &ClickHouseTable,
        tx: Sender<TestSourceMessage>,
    ) -> anyhow::Result<()> {
        let base_url = Self::build_base_url(config)?;

        tx.send(TestSourceMessage {
            error: false,
            done: false,
            message: format!("Testing connection to {base_url}..."),
        })
        .await
        .unwrap();

        // Test basic connectivity with a ping
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()?;

        let ping_url = format!("{base_url}/ping");
        client
            .get(&ping_url)
            .send()
            .await
            .map_err(|e| anyhow!("Failed to connect to ClickHouse at {base_url}: {e}"))?;

        // Test table existence
        let database = config
            .database
            .as_ref()
            .map(|d| d.sub_env_vars())
            .transpose()?
            .unwrap_or_else(|| "default".to_string());

        let check_query = format!(
            "SELECT 1 FROM system.tables WHERE database = '{}' AND name = '{}' LIMIT 1",
            database, table.table_name
        );

        let mut check_url = format!("{}/?query={}", base_url, urlencoding::encode(&check_query));
        if let Some(ref user) = config.username {
            check_url.push_str(&format!("&user={}", urlencoding::encode(&user.sub_env_vars()?)));
        }
        if let Some(ref pass) = config.password {
            check_url.push_str(&format!(
                "&password={}",
                urlencoding::encode(&pass.sub_env_vars()?)
            ));
        }

        let resp = client
            .get(&check_url)
            .send()
            .await
            .map_err(|e| anyhow!("Failed to query ClickHouse: {e}"))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("ClickHouse returned error: {body}");
        }

        Ok(())
    }
}

impl Connector for ClickHouseConnector {
    type ProfileT = ClickHouseConfig;
    type TableT = ClickHouseTable;

    fn name(&self) -> &'static str {
        "clickhouse"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "clickhouse".to_string(),
            name: "ClickHouse".to_string(),
            icon: ICON.to_string(),
            description: "Write results to ClickHouse via HTTP API".to_string(),
            enabled: true,
            source: false,
            sink: true,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(PROFILE_SCHEMA.to_owned()),
            table_config: TABLE_SCHEMA.to_owned(),
        }
    }

    fn test(
        &self,
        _: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let message = match Self::test_connection(&config, &table, tx.clone()).await {
                Ok(_) => TestSourceMessage {
                    error: false,
                    done: true,
                    message: "Successfully connected to ClickHouse".to_string(),
                },
                Err(err) => TestSourceMessage {
                    error: true,
                    done: true,
                    message: format!("{err:?}"),
                },
            };
            tx.send(message).await.unwrap();
        });
    }

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        ConnectionType::Sink
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let base_url = Self::build_base_url(&config)?;
        let description = format!(
            "ClickHouseSink<{}/{}.{}>",
            base_url,
            config
                .database
                .as_ref()
                .map(|d| d.sub_env_vars())
                .transpose()?
                .unwrap_or_else(|| "default".to_string()),
            table.table_name,
        );

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for ClickHouse connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for ClickHouse connection"))?;

        let op_config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            ConnectionType::Sink,
            schema,
            &op_config,
            description,
        ))
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        _profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let host = options.pull_str("host")?;
        let port = options.pull_opt_i64("port")?;
        let username = options.pull_opt_str("username")?.map(VarStr::new);
        let password = options.pull_opt_str("password")?.map(VarStr::new);
        let database = options.pull_opt_str("database")?.map(VarStr::new);
        let https = options.pull_opt_bool("https")?;

        let table_name = options.pull_str("table_name")?;
        let batch_size = options.pull_opt_i64("batch_size")?;
        let batch_flush_interval_ms = options.pull_opt_i64("batch_flush_interval_ms")?;

        let profile = ClickHouseConfig {
            host: VarStr::new(host),
            port,
            username,
            password,
            database,
            https,
        };

        let table = ClickHouseTable {
            table_name,
            batch_size: batch_size.map(|v| v as i64),
            batch_flush_interval_ms: batch_flush_interval_ms.map(|v| v as i64),
        };

        self.from_config(None, name, profile, table, schema)
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        let base_url = Self::build_base_url(&profile)?;
        let insert_url = Self::build_insert_url(&base_url, &profile, &table)?;

        let batch_size = table
            .batch_size
            .map(|v| v as u32)
            .unwrap_or(DEFAULT_BATCH_SIZE);

        let flush_interval_ms = table
            .batch_flush_interval_ms
            .map(|v| v as u64)
            .unwrap_or(DEFAULT_FLUSH_INTERVAL_MS);

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| anyhow!("Failed to build HTTP client: {e}"))?;

        Ok(ConstructedOperator::from_operator(Box::new(
            ClickHouseSinkFunc::new(
                insert_url,
                client,
                batch_size,
                flush_interval_ms,
                ArrowSerializer::new(
                    config
                        .format
                        .expect("No format configured for ClickHouse sink"),
                ),
            ),
        )))
    }
}
