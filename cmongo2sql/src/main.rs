/*
cmongo2sql
Utility to convert a MongoDB JSON dump to a SQL dump.

Copyright 2015 Sam Saint-Pettersen.
Licensed under the MIT/X11 License.

Rust port of original Python tool (1.0.5).
*/

extern crate clioptions;
extern crate regex;
extern crate rustc_serialize;
use clioptions::CliOptions;
use regex::Regex;
use rustc_serialize::json::Json;
use std::io::{BufRead, BufReader, Write};
use std::fs::File;
use std::process::exit;

fn convert_json_to_sql(input: &str, output: &str, db: &str, comments: bool, verbose: bool) {
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

    for record in records {
        let r = record.as_object();
        for p in r {
            for (k, v) in p.iter() {
                println!("{:?} => {:?}", k, v);

                if v.is_object() {
                    println!("Object.");
                }
                else if v.is_string() {
                    println!("String.");
                }
                else if v.is_number() {
                    println!("Number.");
                }
            }
        }
    }
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

fn display_version() {
    let signature = "cmongo2sql 1.0.0 (https://github.com/stpettersens/db-tools)";
    println!("{}", signature);
    exit(0);
}

fn check_extensions(program: &str, input: &str, output: &str) {
    let mut re = Regex::new(r".sql$").unwrap();
    if !re.is_match(&input) {
        display_error(&program, &format!("Input file '{}' is not SQL", &input));
    }
    re = Regex::new(r".json$").unwrap();
    if !re.is_match(&output) {
        display_error(&program, &format!("Output file '{}' is not JSON", &output));
    }
}

fn main() {
	let cli = CliOptions::new("csql2mongo");
    let program = cli.get_program();
    //let args = cli.get_args();
    let mut input = String::new();
    let mut output = String::new();
    let mut db = String::new();
    let mut comments = true;
    //let mut extensions = true;
    let mut verbose = false;

    /*if cli.get_num() > 1 {
        for (i, a) in args.iter().enumerate() {

        }
    }*/
    let x = true;
    if x {
        input = "sample.json".to_string();
        output = "out.sql".to_string();
        convert_json_to_sql(&input, &output, &db, comments, verbose);
    }
    else {
        display_error(&program, "No options specified"); 
    }
}
