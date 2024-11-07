use actix_web::{get, post, App, HttpResponse, HttpServer, Responder};
use actix_cors::Cors;
use csv::ReaderBuilder;
use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use std::io::Cursor;

#[derive(Serialize)]
struct CSVRow(HashMap<String, String>);

#[get("/")]
async fn hello_world() -> &'static str {
    "Api em Rust rodando"
}

#[post("/csv_to_sql")]
async fn csv_to_sql(csv_data: String) -> impl Responder {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(Cursor::new(csv_data));

    // Lê os headers do CSV
    let headers = match reader.headers() {
        Ok(headers) => headers.clone(),
        Err(_) => return HttpResponse::BadRequest().body("Erro ao processar CSV."),
    };

    let mut sql_statements = Vec::new();
    
    // Gerar instrução SQL de inserção
    for result in reader.records() {
        match result {
            Ok(record) => {
                let values: Vec<String> = record.iter()
                    .map(|value| format!("'{}'", value)) // Adiciona aspas para os valores
                    .collect();

                let sql = format!(
                    "INSERT INTO tabela ({}) VALUES ({});",
                    headers.iter().collect::<Vec<&str>>().join(", "),
                    values.join(", ")
                );
                sql_statements.push(sql);
            }
            Err(_) => return HttpResponse::BadRequest().body("Erro ao processar CSV."),
        }
    }

    let sql_data = sql_statements.join("\n");

    // Retornar o arquivo SQL como resposta
    HttpResponse::Ok()
        .insert_header(("Content-Disposition", "attachment; filename=output.sql"))
        .content_type("application/sql")
        .body(sql_data)
}

#[post("/csv_to_json")]
async fn csv_to_json(csv_data: String) -> impl Responder {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(Cursor::new(csv_data));

    let headers = match reader.headers() {
        Ok(headers) => headers.clone(),
        Err(_) => return HttpResponse::BadRequest().body("Erro ao processar CSV."),
    };

    let mut rows = Vec::new();

    for result in reader.records() {
        match result {
            Ok(record) => {
                let mut row = HashMap::new();
                for (header, value) in headers.iter().zip(record.iter()) {
                    row.insert(header.to_string(), value.to_string());
                }
                rows.push(CSVRow(row));
            }
            Err(_) => return HttpResponse::BadRequest().body("Erro ao processar CSV."),
        }
    }

    let json_data = json!(rows).to_string();

    HttpResponse::Ok()
        .insert_header(("Content-Disposition", "attachment; filename=output.json"))
        .content_type("application/json")
        .body(json_data)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600); // Adicione o tempo máximo para o cache do CORS

        App::new()
            .wrap(cors)
            .service(csv_to_json)
            .service(csv_to_sql)
            .service(hello_world)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
