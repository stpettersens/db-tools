/*
cmongo2sql
Utility to convert a MongoDB JSON dump to a SQL dump.

Copyright 2016 Sam Saint-Pettersen.
Licensed under the MIT/X11 License.

Rust port of original Python tool (1.0.5).
*/

extern crate clioptions;
extern crate regex;
extern crate rustc_serialize;
extern crate chrono;
use clioptions::CliOptions;
use regex::Regex;
use rustc_serialize::json::Json;
use chrono::*;
use std::io::{BufRead, BufReader, Write};
use std::fs::File;
use std::process::exit;

fn is_date(v: &str) -> bool {
    let re = Regex::new("T").unwrap();
    let mut date = false;
    if re.is_match(&v) {
        date = true;
    }
    date
}

fn parse_date(v: &str) -> String {
    let mut re = Regex::new("T").unwrap();
    let mut p = v.to_string();
    p = re.replace(&p, " ");
    re = Regex::new(r"\.\d{3}Z").unwrap();
    p = re.replace(&p, "");
    re = Regex::new(r"\.\d{3}\+\d{4}").unwrap();
    p = re.replace(&p, "");
    p
}

fn convert_json_to_sql(program: &str, signature: &str, input: &str, output: &str, db: &str, 
comments: bool, verbose: bool) {
    let mut lines: Vec<String> = Vec::new();
    let f = File::open(input).unwrap();
    let file = BufReader::new(&f);
    for line in file.lines() {
        lines.push(line.unwrap());
    }

    let mut records = Vec::new();
    for line in lines {
        records.push(Json::from_str(&line).unwrap());
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

    for record in records {
        let r = record.as_object();
        for c in r {
            for (k, v) in c.iter() {
                if v.is_object() {
                    let v = format!("{}", v);
                    let re = Regex::new("(.[^:]*):([\"-\\.:_A-Za-z0-9]*)").unwrap();
                    for cap in re.captures_iter(&v) {
                        let t = format!("{}", cap.at(1).unwrap().to_string());
                        let mut d = format!("{}", cap.at(2).unwrap().to_string());
                        
                        let t = &t[2..t.len()-1]; // Strip type of '{"' & '}'.

                        if t == "$oid" && !fields.contains(&k) {
                            fields.push(format!("{}", k));
                            ctable.push(format!("`{}` VARCHAR(24),", k));
                        }
                        else if t == "$date" && !field.contains(&k) {
                            if !fields.contains(&k) {
                                fields.push(format!("{}", k));
                                ctable.push(format!("`{}` TIMESTAMP,", k));
                            }
                            d = parse_date(&d);
                        }
                        inserts.push(format!("{},", d));
                    }
                }
                else if v.is_string() {
                    let v = format!("{}", v);
                    if !fields.contains(&k) {
                        fields.push(format!("{}", k));
                        if !is_date(&v) {
                            let mut length = 50;
                            if k == "description" {
                                length = 100;
                            }
                            ctable.push(format!("`{}` VARCHAR({}),", k, length));
                        }
                        else {
                            ctable.push(format!("`{}` TIMESTAMP,", k));
                        }
                    }
                    if is_date(&v) {
                        inserts.push(format!("{}", parse_date(&v)));
                    }
                    else {
                        inserts.push(format!("{},", v));
                    }
                }
                else if v.is_number() {
                    if !fields.contains(&k) {
                        fields.push(format!("{}", k));
                        ctable.push(format!("`{}` NUMERIC(15, 2),", k));
                    }
                    let v = format!("{}", v);
                    let n = v.parse::<f64>().ok();
                    let v = match n {
                        Some(v) => v,
                        None => {
                            display_error(&program, "Problem converting to numeric value");
                            return;
                        }
                    };
                    inserts.push(format!("{:.*},", 2, v));
                }
                else if v.is_boolean() {
                    if !fields.contains(&k) {
                        fields.push(format!("{}", k));
                        ctable.push(format!("`{}` BOOLEAN,", k));
                    }
                    let v = format!("{}", v);
                    inserts.push(format!("{},", v.to_uppercase()));
                }
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
        sql.push(format!("-- SQL table dump from MongoDB collection: {} ({} -> {})", table, input, output));
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
        println!("MongoDB JSON dump file: '{}'.\n", input);
    }

    let mut w = File::create(output).unwrap();
    let _ = w.write_all(sql.join("\n").as_bytes());
}

fn display_error(program: &str, err: &str) {
    println!("Error: {}.", err);
    display_usage(&program, -1);
}

fn display_usage(program: &str, code: i32) {
    println!("\ncmongo2sql");
    println!("Utility to convert a MongoDB JSON dump to a SQL dump.");
    println!("\nCopyright 2016 Sam Saint-Pettersen.");
    println!("Licensed under the MIT/X11 License.");
    println!("\nUsage: {} -f|--file <input.json> -o|--out <output.sql>", program);
    println!("-d|--db <database> -n|--no-comments -i|--ignore-ext -l|--verbose [-v|--version][-h|--help]");
    println!("\n-f|--file: MongoDB JSON file to convert.");
    println!("-o|--out: SQL file as output.");
    println!("-d|--db: Databse name to use for output.");
    println!("-n|--no-comments: Do not write comments in output.");
    println!("-i|--ignore-ext: Ignore file extensions for input/output.");
    println!("-l|--verbose: Display console output on conversion.");
    println!("-v|--version: Display program version and exit.");
    println!("-h|--help: Display this help information and exit.");
    exit(code);
}

fn display_version(signature: &str) {
    println!("{}", signature);
    exit(0);
}

fn check_extensions(program: &str, input: &str, output: &str) {
    let mut re = Regex::new(r".json$").unwrap();
    if !re.is_match(&input) {
        display_error(&program, &format!("Input file '{}' is not JSON", &input));
    }
    re = Regex::new(r".sql$").unwrap();
    if !re.is_match(&output) {
        display_error(&program, &format!("Output file '{}' is not SQL", &output));
    }
}

fn main() {

    let signature = "cmongo2sql 1.0.0 (https://github.com/stpettersens/db-tools)";

    let cli = CliOptions::new("csql2mongo");
    let program = cli.get_program();
    let args = cli.get_args();
    let mut input = String::new();
    let mut output = String::new();
    let mut db = String::new();
    let mut comments = true;
    let mut extensions = true;
    let mut verbose = false;

    if cli.get_num() > 1 {
        for (i, a) in args.iter().enumerate() {
            if a == "-h" || a == "--help" {
                display_usage(&program, 0);
            }
            else if a == "-v" || a == "--version" {
                display_version(&signature);
            } 

            if a == "-f" || a == "--file" {
                input = cli.next_argument(i);
            }
            if a == "-o" || a == "--out" {
                output = cli.next_argument(i);
            }
            if a == "-d" || a == "--db" {
                db = cli.next_argument(i);
            }
            if a == "-n" || a == "--no-comments" {
                comments = false;
            }
            if a == "-i" || a == "--ignore-ext" {
                extensions = false;
            }
            if a == "-l" || a == "--verbose" {
                verbose = true;
            }
        }

        if extensions {
            check_extensions(&program, &input, &output);
        }

        if input.len() > 0 && output.len() > 0 {
            convert_json_to_sql(&program, &signature, &input, &output, &db, comments, verbose);
        }
        else if input.len() == 0 {
            display_error(&program, "No input file specified");
        }
        else if output.len() == 0 {
            display_error(&program, "No output file specified");
        }
        else {
            display_error(&program, "Incomplete options provided");
        }
    }
    else {
        display_error(&program, "No options specified"); 
    }
}
