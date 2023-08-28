use std::collections::HashMap;
use std::time::Duration;

use async_nats::ConnectOptions;
use serde::de::{self, Visitor};
use serde_yaml;

/// NATS connection base yml file format!

#[derive(Debug)]
pub enum AuthType {
    None, // No authentication required,  reads connection string from environment variable
    WithToken,
    WithUserAndPassword,
    WithNkey,
    WithJwt,
    WithCredentialsFile,
    WithCredentials,
}

impl AuthType {
    pub fn from(auth_type: Option<String>) -> AuthType {
        match auth_type {
            Some(auth_type) => match auth_type.as_str() {
                "with_token" => AuthType::WithToken,
                "with_user_and_password" => AuthType::WithUserAndPassword,
                "with_nkey" => AuthType::WithNkey,
                "with_jwt" => AuthType::WithJwt,
                "with_credentials_file" => AuthType::WithCredentialsFile,
                "with_credentials" => AuthType::WithCredentials,
                _ => panic!("{} is not a valid auth_type", auth_type),
            },
            None => AuthType::None,
        }
    }
}

/// NATS connection with WITH_TOKEN
/// auth_type: 'with_token'
/// token: 'token'
/// ... other options

/// NATS connection with WITH_USER_AND_PASSWORD
/// auth_type: 'with_user_and_password'
/// user: 'user'
/// password: 'password'
/// ... other options

/// NATS connection with WITH_NKEY
/// auth_type: 'with_nkey'
/// nkey: 'nkey'
/// ... other options

/// NATS connection with WITH_JWT
/// auth_type: 'with_jwt'
/// jwt: 'jwt'
/// ... other options

/// NATS connection with WITH_CREDENTIALS_FILE
/// auth_type: 'with_credentials_file'
/// credentials_file: 'credentials_file'
/// ... other options

/// NATS connection with WITH_CREDENTIALS
/// auth_type: 'with_credentials'
/// credentials: 'credentials'
/// ... other ConnectOptions

const FIELDS: [&str; 21] = [
    "auth_type",
    "name",
    "no_echo",
    "retry_on_failed_connect",
    "max_reconnects",
    "reconnect_buffer_size",
    "connection_timeout",
    "tls_required",
    "certificates",
    "client_cert",
    "client_key",
    "flush_interval",
    "ping_interval",
    "subscription_capacity",
    "sender_capacity",
    "inbox_prefix",
    "request_timeout",
    "retry_on_initial_connect",
    "ignore_discovered_servers",
    "retain_servers_order",
    "read_buffer_capacity",
];

struct SerializableNatsConnect {
    auth_type: Option<AuthType>,
    name: Option<String>,
    no_echo: Option<bool>,
    retry_on_failed_connect: Option<bool>,
    max_reconnects: Option<usize>,
    reconnect_buffer_size: Option<usize>,
    connection_timeout: Option<Duration>, // in second std::time::Duration::in_seconds
    // auth: Option<Auth>,
    tls_required: Option<bool>,
    certificates: Option<Vec<String>>, // string -> PathBuf
    client_cert: Option<String>,       // string -> PathBuf
    client_key: Option<String>,        // string -> PathBuf
    // tls_client_config: Option<rustls::ClientConfig>,
    flush_interval: Option<Duration>, // in second std::time::Duration::from_milliseconds
    ping_interval: Option<Duration>,  // in second std::time::Duration::in_seconds
    subscription_capacity: Option<usize>,
    sender_capacity: Option<usize>,
    // event_callback: Option<CallbackArg1<Event, ()>>,
    inbox_prefix: Option<String>,
    request_timeout: Option<Duration>, // in second std::time::Duration::in_seconds
    retry_on_initial_connect: Option<bool>,
    ignore_discovered_servers: Option<bool>,
    retain_servers_order: Option<bool>,
    read_buffer_capacity: Option<u16>,
    // reconnect_delay_callback: Option<Box<dyn Fn(usize) -> Duration + Send + Sync + 'static>>,
    // auth_callback: Option<CallbackArg1<Vec<u8>, Result<Auth, AuthError>>>,
    token: Option<String>,
    user: Option<String>,
    password: Option<String>,
    nkey: Option<String>,
    jwt: Option<String>,
    credentials_file: Option<String>,
    credentials: Option<HashMap<String, String>>,
    servers: Option<Vec<String>>,
}

// Maybe Serde takes care of this??
// should check
fn str_to_bool(str: Option<String>) -> Option<bool> {
    if None == str {
        return None;
    }
    match str.unwrap().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => panic!("{} Cannot be parsed to a boolean", str.unwrap()),
    }
}

fn u16_seconds_to_duration(str: Option<u16>) -> Option<Duration> {
    if None == str {
        return None;
    }
    Some(Duration::from_secs(str.unwrap_or_default() as u64))
}

fn u16_millis_to_duration(str: Option<u16>) -> Option<Duration> {
    if None == str {
        return None;
    }
    Some(Duration::from_millis(str.unwrap_or_default() as u64))
}

impl SerializableNatsConnect {
    pub fn to_connect_options(&self) -> ConnectOptions {
        let auth_type = self.auth_type;
        let mut c = ConnectOptions::default();
        if self.name.is_some() {
            c = c.name(self.name.unwrap());
        }

        // map normal ConnectOptions
        //
        match auth_type.unwrap() {
            AuthType::None => {}
            AuthType::WithToken => {
                c.token(self.token.unwrap());
            }
            AuthType::WithUserAndPassword => {
                c.user_and_password(self.user.unwrap(), self.password.unwrap());
            }
            AuthType::WithNkey => {
                c.nkey(self.nkey.unwrap());
            }
            // Todo:
            // looks like need callback
            // AuthType::WithJwt => {
            // c.jwt(self.jwt.unwrap());
            // }
            AuthType::WithCredentialsFile => {
                c = ConnectOptions::with_credentials_file(self.credentials_file.unwrap().into());
            }
            AuthType::WithCredentials => {
                c.credentials(self.credentials.unwrap());
            }
        }
        c
        // to be implemented
    }
}

// todo: implement deserializer
impl<'de> Visitor<'de> for SerializableNatsConnect {
    type Value = SerializableNatsConnect;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("struct SerializableNatsConnect")
    }
    fn visit_map<V>(mut self, mut map: V) -> Result<Self::Value, V::Error>
    where V: de::MapAccess<'de> {
        while let Some((key, value)) = map.next_entry()? {
            match key {
                "auth_type" => {
                    let auth_type: Option<String> = map.next_value()?;
                    self.auth_type = Some(AuthType::from(auth_type));
                }
                "name" => {
                    self.name = value;
                }
                "no_echo" => {
                    self.no_echo = map.next_value()?;
                    // let no_echo: Option<String> = map.next_value()?;
                    // self.no_echo = str_to_bool(no_echo);
                }
                "retry_on_failed_connect" => {
                    self.retry_on_failed_connect = map.next_value()?;
                    // let retry_on_failed_connect: Option<String> = map.next_value()?;
                    // self.retry_on_failed_connect = str_to_bool(retry_on_failed_connect);
                }
                "max_reconnects" => {
                    let max_reconnects: Option<u16> = map.next_value()?;
                    self.max_reconnects = map.next_value()?;
                }
                "reconnect_buffer_size" => {
                    self.reconnect_buffer_size = map.next_value()?;
                }
                "connection_timeout" => {
                    let connection_timeout: Option<u16> = map.next_value()?;
                    self.connection_timeout = u16_seconds_to_duration(connection_timeout);
                }
                "tls_required" => {
                    self.tls_required = map.next_value()?;
                    // let tls_required: Option<String> = map.next_value()?;
                    // self.tls_required = str_to_bool(tls_required);
                }
                "certificates" => {
                    self.certificates = map.next_value()?;
                }
                "client_cert" => {
                    self.client_cert = map.next_value()?;
                }
                "client_key" => {
                    self.client_key = map.next_value()?;
                }
                "flush_interval" => {
                    let flush_interval: Option<u16> = map.next_value()?;
                    self.flush_interval = u16_millis_to_duration(flush_interval);
                }
                "ping_interval" => {
                    let ping_interval: Option<u16> = map.next_value()?;
                    self.ping_interval = u16_seconds_to_duration(ping_interval);
                }
                "subscription_capacity" => {
                    self.subscription_capacity = map.next_value()?;
                }
                "sender_capacity" => {
                    self.sender_capacity = map.next_value()?;
                }
                "inbox_prefix" => {
                    self.inbox_prefix = map.next_value()?;
                }
                "request_timeout" => {
                    let request_timeout: Option<u16> = map.next_value()?;
                    self.request_timeout = u16_seconds_to_duration(request_timeout);
                }
                "retry_on_initial_connect" => {
                    self.retry_on_initial_connect = map.next_value()?;
                    // let retry_on_initial_connect: Option<String> = map.next_value()?;
                    // self.retry_on_initial_connect = str_to_bool(retry_on_initial_connect);
                }
                "ignore_discovered_servers" => {
                    self.ignore_discovered_servers = map.next_value()?;
                    // let ignore_discovered_servers: Option<String> = map.next_value()?;
                    // self.ignore_discovered_servers = str_to_bool(ignore_discovered_servers);
                }
                "retain_servers_order" => {
                    self.retain_servers_order = map.next_value()?;
                    // let retain_servers_order: Option<String> = map.next_value()?;
                    // self.retain_servers_order = str_to_bool(retain_servers_order)
                }
                "read_buffer_capacity" => {
                    self.read_buffer_capacity = map.next_value()?;
                }
                "token" => {
                    self.token = map.next_value()?;
                }
                "user" => {
                    self.user = map.next_value()?;
                }
                "password" => {
                    self.password = map.next_value()?;
                }
                "nkey" => {
                    self.nkey = map.next_value()?;
                }
                "jwt" => {
                    self.jwt = map.next_value()?;
                }
                "credentials_file" => {
                    self.credentials_file = map.next_value()?;
                }
                "credentials" => {
                    self.credentials = map.next_value()?;
                }
                "servers" => {
                    self.servers = map.next_value()?;
                }
                _ => {
                    return Err(de::Error::unknown_field(key, &FIELDS));
                }
            }
        }
        Ok(self)
    }
}

impl SerializableNatsConnect {
    pub fn validate(&self) -> anyhow::Result<(), anyhow::Error> {
        let auth_type = self.auth_type;

        if self.servers.is_none() || self.servers.unwrap().is_empty() {
            return Err(anyhow::Error::msg("servers must be provided"));
        }
        // if auth is none, its okay
        if auth_type.is_none() {
            return Ok(());
        }

        // if auth is not none, check if the auth_type is supported
        match auth_type.unwrap() {
            AuthType::WithToken => {
                let token = self.token.unwrap();
                if token.is_empty() {
                    return Err(anyhow::Error::msg("token cannot be empty"));
                }
            }
            AuthType::WithUserAndPassword => {
                let user = self.user.unwrap();
                let password = self.password.unwrap();
                if user.is_empty() {
                    return Err(anyhow::Error::msg("user cannot be empty"));
                }
                if password.is_empty() {
                    return Err(anyhow::Error::msg("password cannot be empty"));
                }
            }
            AuthType::WithNkey => {
                let nkey = self.nkey.unwrap();
                if nkey.is_empty() {
                    return Err(anyhow::Error::msg("nkey cannot be empty"));
                }
            }
            AuthType::WithJwt => {
                let jwt = self.jwt.unwrap();
                if jwt.is_empty() {
                    return Err(anyhow::Error::msg("jwt cannot be empty"));
                }
            }
            AuthType::WithCredentialsFile => {
                let credentials_file = self.credentials_file.unwrap();
                if credentials_file.is_empty() {
                    return Err(anyhow::Error::msg("credentials_file cannot be empty"));
                }
            }
            AuthType::WithCredentials => {
                let credentials = self.credentials.unwrap();
                if credentials.is_empty() {
                    return Err(anyhow::Error::msg("credentials cannot be empty"));
                }
            }
            _ => {
                return Err(anyhow::Error::msg("auth_type is not supported"));
            }
        }
        Ok(())
    }
}

struct NatsConnectOptions {
    file: String,
}

impl NatsConnectOptions {
    pub fn new(file: String) -> Self {
        Self { file }
    }
}

impl From<NatsConnectOptions> for ConnectOptions {
    fn from(options: NatsConnectOptions) -> anyhow::Result<ConnectOptions, anyhow::Error> {
        let file = options.file;
        let file_content = std::fs::read_to_string(file).unwrap();
        let data: SerializableNatsConnect = serde_yaml::from_str(&file_content).unwrap();
        data.validate()?;
        Ok(data.to_connect_options())
    }
}
