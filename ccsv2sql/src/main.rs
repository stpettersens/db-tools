/*
ccsv2sql
Utility to convert a CSV file to a SQL dump.

Copyright 2016 Sam Saint-Pettersen.
Licensed under the MIT/X11 License.

Rust port of original Python tool (1.0.6).
*/

extern crate clioptions;
extern crate regex;
extern crate csv;
extern crate chrono;
use clioptions::CliOptions;
use regex::Regex;
use chrono::*;
use std::io::{BufRead, BufReader, Write};
use std::fs::File;
use std::process::exit;

fn parse_timestamp(v: &str) -> String {
    let re = Regex::new(r"(\d{4}-\d{2}-\d{2}[T\s]*\d{2}:\d{2}:\d{2}).*").unwrap();
    let mut p = String::new();
    for cap in re.captures_iter(&v) {
        p = cap.at(1).unwrap().to_string();
        let re = Regex::new("T").unwrap();
        p = re.replace(&p, " "); 
    }
    p
}

fn convert_csv_to_json(signature: &str, input: &str, output: &str, separator: &str, db: &str,
comments: bool, verbose: bool) {
    let f = File::open(input).unwrap();
    let file = BufReader::new(&f);
    let mut headers = String::new();
    for line in file.lines() {
        for h in line {
            headers = h;
        }
        break;
    }
    
    let hi = headers.split(separator);
    let mut headers = Vec::new();
    for h in hi {
        headers.push(format!("{}", h));
    }

    let mut file = csv::Reader::from_file(&input).unwrap();
    let mut records: Vec<Vec<(String)>> = Vec::new();
    for r in file.records().map(|r| r.unwrap()) {
        records.push(r);
    }

    let mut table = String::new();
    let re = Regex::new(r"(.*)\..{3,4}").unwrap();
    for cap in re.captures_iter(&input) {
        table = cap.at(1).unwrap().to_string();
    }

    let dtable = format!("DROP TABLE IF EXISTS `{}`;", table);
    let insert = format!("INSERT INTO `{}` VALUES (", table);
    let mut ctable: Vec<String> = Vec::new();
    let mut inserts: Vec<String> = Vec::new();
    let mut fields: Vec<String> = Vec::new();

    ctable.push(format!("CREATE TABLE IF NOT EXISTS `{}` (", table));

    for r in records {
        for (i, v) in r.iter().enumerate() {
            // ObjectIds
            let mut re = Regex::new("ObjectId((.*))").unwrap();
            if re.is_match(&v) {
                if i < headers.len() && !fields.contains(&headers[i]) {
                    fields.push(headers[i].clone());
                    ctable.push(format!("`{}` VARCHAR(24),", headers[i]));
                }

                for cap in re.captures_iter(&v) {
                    let v = cap.at(1).unwrap().to_string();
                    inserts.push(format!("\"{}\"", &v[1..v.len() - 1]));
                }
                continue;
            }
            // Dates
            re = Regex::new(r"(\d{4}-\d{2}-\d{2}.*)").unwrap();
            if re.is_match(&v) {
                if i < headers.len() && !fields.contains(&headers[i]) {
                    fields.push(headers[i].clone());
                    ctable.push(format!("`{}` TIMESTAMP,", headers[i]));
                }
                for cap in re.captures_iter(&v) {
                    let v = cap.at(1).unwrap().to_string();
                    inserts.push(format!("\"{}\",", parse_timestamp(&v[0..v.len() - 1])));
                }
                continue;
            }
            // Numbers
            re = Regex::new(r"\d+.*").unwrap();
            if re.is_match(&v) {
                if i < headers.len() && !fields.contains(&headers[i]) {
                    fields.push(headers[i].clone());
                    ctable.push(format!("`{}` NUMERIC(15, 2),", headers[i]));
                }
                let n = v.parse::<f32>().ok();
                let v = match n {
                    Some(v) => v,
                    None => 0 as f32
                };
                inserts.push(format!("{:.*},", 2, v));
                continue;
            }
            // Booleans
            re = Regex::new("TRUE|true|FALSE|false").unwrap();
            if re.is_match(&v) {
                if i < headers.len() && !fields.contains(&headers[i]) {
                    fields.push(headers[i].clone());
                    ctable.push(format!("`{}` BOOLEAN,", headers[i]));
                }
                inserts.push(format!("{},", v.to_string().to_uppercase()));
                continue;
            }
            // Strings
            re = Regex::new(r"\w+").unwrap();
            if re.is_match(&v) {
                if i < headers.len() && !fields.contains(&headers[i]) {
                    fields.push(headers[i].clone());
                    let mut length = 50;
                    if headers[i] == "description" {
                        length = 100;
                    }
                    ctable.push(format!("`{}` VARCHAR({}),", headers[i], length));
                }
                inserts.push(format!("\"{}\",", v));
                continue;
            }
        }
    }

    let mut last = ctable[ctable.len() - 1].clone().to_string();
    last = format!("{});", &last[0..last.len() - 1]);
    let index = ctable.len() - 1;
    ctable[index] = last;

    let mut records: Vec<Vec<String>> = Vec::new();
    let v = &inserts;
    for record in v.chunks(fields.len()) {
        let no_comma = fields.len() - 1;
        let mut r: Vec<String> = Vec::new();
        r.push(insert.clone());
        for (i, c) in record.iter().enumerate() {
            if i < no_comma {
                r.push(c.to_string());
            }
            else {
                r.push(format!("{});", &c[0..c.len() - 1]));
            }
        }
        records.push(r);
    }

    let mut sql: Vec<String> = Vec::new();
    if comments {
        let timestamp: DateTime<Local> = Local::now();
        sql.push(format!("-- SQL table dump from CSV file: {} ({} -> {})", table, input, output));
        sql.push(format!("-- Generated by: {}", signature));
        sql.push(format!("-- Generated at: {}", timestamp));
        sql.push(String::new());
    }
    if db.len() > 0 {
        sql.push(format!("USE `{}`;", db));
    }
    sql.push(dtable);
    sql.append(&mut ctable);
    sql.push(String::new());
    for r in records {
        for f in r {
            sql.push(f);
        }
        sql.push(String::new());
    }

    if verbose {
        println!("\nGenerating SQL dump file: '{}' from", output);
        println!("CSV file: '{}'.\n", input);
    }

    let mut w = File::create(output).unwrap();
    let _ = w.write_all(sql.join("\n").as_bytes());
}

fn check_extensions(program: &str, input: &str, output: &str) {
    let mut re = Regex::new(r".csv$").unwrap();
    if !re.is_match(&input) {
        display_error(&program, &format!("Input file '{}' is not CSV", &input));
    }
    re = Regex::new(r".sql$").unwrap();
    if !re.is_match(&output) {
        display_error(&program, &format!("Output file '{}' is not SQL", &output));
    }
}

fn display_error(program: &str, err: &str) {
    println!("Error: {}.", err);
    display_usage(&program, -1);
}

fn display_version(signature: &str) {
    println!("{}", signature);
    exit(0);
}

fn display_usage(program: &str, code: i32) {
    println!("\nccsv2sql");
    println!("Utility to convert a CSV file to a SQL dump.");
    println!("\nCopyright 2016 Sam Saint-Pettersen.");
    println!("Licensed under the MIT/X11 License.");
    println!("\nUsage: {} -f|--file <input.csv> -o|--out <output.sql> -s|--separator <separator>", program);
    exit(code);
}

fn main() {

    let signature = "ccsv2sql 1.0.0 (https://github.com/stpettersens/db-tools)";
    
    let cli = CliOptions::new("ccsv2mongo");
    let program = cli.get_program();

    let mut input = String::new();
    let mut output = String::new();
    let mut separator = ",".to_string();
    let mut db = String::new();
    let mut comments = true;
    let mut extensions = true;
    let mut verbose = false;

    if cli.get_num() > 1 {
        for (i, a) in cli.get_args().iter().enumerate() {
            match a.trim() {
                "-h" | "--help" => display_usage(&program, 0),
                "-v" | "--version" => display_version(&signature),
                "-f" | "--file" => input = cli.next_argument(i),
                "-o" | "--out" => output = cli.next_argument(i),
                "-s" | "--separator" => separator = cli.next_argument(i),
                "-d" | "--db" => db = cli.next_argument(i),
                "-n" | "--no-comments" => comments = false,
                "-i" | "--ignore-ext" => extensions = false,
                "-l" | "--verbose" => verbose = true,
                _ => continue,
            }
        }

        if extensions {
            check_extensions(&program, &input, &output);
        }
  
        if input.is_empty() {
            display_error(&program, "No input file specified");
        }
        else if output.is_empty() {
            display_error(&program, "No output file specified");
        }

        convert_csv_to_json(&signature, &input, &output, &separator, &db, comments, verbose);
    }
    else {
        display_error(&program, "No options specified"); 
    }
}
