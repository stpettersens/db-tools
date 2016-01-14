/*
cmongo2csv
Utility to convert a MongoDB JSON dump to a CSV file.

Copyright 2016 Sam Saint-Pettersen.
Licensed under the MIT/X11 License.

Rust port of original Python tool (1.0.1).
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

fn is_date(v: &str) -> bool {
    let re = Regex::new(r"\d{4}-\d{2}-\d{2}.*").unwrap();
    let mut date = false;
    if re.is_match(&v) {
        date = true;
    }
    date
}

fn parse_timestamp(v: &str, tz: bool) -> String {
    let re = Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}).*").unwrap();
    let mut p = String::new();
    for cap in re.captures_iter(&v) {
    	p = cap.at(1).unwrap().to_string();
    }
    if tz {
    	p = format!("{}Z", p);
    }
    else {
    	p = format!("{}+0000", p);
    }
    p
}

fn convert_json_to_csv(input: &str, output: &str, separator: &str, tz: bool, mongo_types: bool, verbose: bool) {
    let mut records = Vec::new();
    let f = File::open(input).unwrap();
    let file = BufReader::new(&f);
    for line in file.lines() {
        records.push(Json::from_str(&line.unwrap()).unwrap());
    }

    let mut inserts: Vec<String> = Vec::new();
    let mut fields: Vec<String> = Vec::new();

    for record in records {
        let r = record.as_object();
        for c in r {
            for (k, v) in c.iter() {
                if v.is_object() {
                    let v = format!("{}", v);
                    let re = Regex::new("(.[^:]*):([\"-\\.:_A-Za-z0-9]*)").unwrap();
                    for cap in re.captures_iter(&v) {
                        let t = format!("{}", cap.at(1).unwrap().to_string());
                        let d = format!("{}", cap.at(2).unwrap().to_string());
                        
                        let t = &t[2..t.len()-1]; // Strip type of '{"' & '}'.

                        if !fields.contains(&k) {
                        	fields.push(format!("{}", k));
                        }

                        if t == "$oid" {
                        	if mongo_types {
                            	inserts.push(format!("ObjectId({})", &d[1..d.len()-1]));
                            }
                            else {
                            	inserts.push(format!("{}", &d[1..d.len()-1]));
                            }
                        }
                        else if t == "$date" {
                            inserts.push(format!("{}", parse_timestamp(&d[1..d.len()-1], tz)));
                        }
                    }
                }
                else if v.is_string() {
                    let v = format!("{}", v);
                    if !fields.contains(&k) {
                        fields.push(format!("{}", k));
                    }
                    if is_date(&v) && mongo_types {
                        inserts.push(format!("{}", parse_timestamp(&v, tz)));
                    }
                    else {
                        inserts.push(format!("{}", &v[1..v.len()-1]));
                    }
                }
                else if v.is_number() {
                    if !fields.contains(&k) {
                        fields.push(format!("{}", k));
                    }
                    let v = format!("{}", v);
                    let n = v.parse::<f32>().ok();
                    let v = match n {
                        Some(v) => v,
                        None => 0 as f32
                    };
                    inserts.push(format!("{:.*}", 2, v));
                }
                else if v.is_boolean() {
                    if !fields.contains(&k) {
                        fields.push(format!("{}", k));
                    }
                    let mut v = format!("{}", v);
                    v = v.to_lowercase();
                    if mongo_types {
                    	v = format!("%!s(bool={})", v);
                    }
                    inserts.push(format!("{}", v));
                }
            }
        }
    }

    let mut csv: Vec<String> = Vec::new();
    csv.push(fields.join(separator));

   	let v = &inserts;
    for r in v.chunks(fields.len()) {
    	csv.push(r.join(separator));
    }
    csv.push(String::new());

    if verbose {
        println!("\nGenerating CSV file: '{}' from", output);
        println!("MongoDB JSON dump file: '{}'.\n", input);
    }

    let mut w = File::create(output).unwrap();
    let _ = w.write_all(csv.join("\n").as_bytes());
}

fn check_extensions(program: &str, input: &str, output: &str) {
    let mut re = Regex::new(r".json$").unwrap();
    if !re.is_match(&input) {
        display_error(&program, &format!("Input file '{}' is not JSON", &input));
    }
    re = Regex::new(r".csv$").unwrap();
    if !re.is_match(&output) {
        display_error(&program, &format!("Output file '{}' is not CSV", &output));
    }
}

fn display_error(program: &str, err: &str) {
    println!("Error: {}.", err);
    display_usage(&program, -1);
}

fn display_version() {
	let signature = "cmongo2csv 1.0.0 (https://github.com/stpettersens/db-tools)";
    println!("{}", signature);
    exit(0);
}

fn display_usage(program: &str, code: i32) {
    println!("\ncmongo2sql");
    println!("Utility to convert a MongoDB JSON dump to a CSV file.");
    println!("\nCopyright 2016 Sam Saint-Pettersen.");
    println!("Licensed under the MIT/X11 License.");
    println!("\nUsage: {} -f|--file <input.json> -o|--out <output.csv> -s|--separator <separator>", program);
    println!("-i|--ignore-ext -l|--verbose [-v|--version][-h|--help]");
    println!("\n-f|--file: MongoDB JSON file to convert.");
    println!("-o|--out: CSV file as output.");
    println!("-s|--separator: Separator to use in output (default: ,).");
    println!("-t|--tz: Use \"Z\" as timezone for timestamps rather than +0000");
    println!("-n|--no-mongo-types: Do not use MongoDB types in output.");
    println!("-i|--ignore-ext: Ignore file extensions for input/output.");
    println!("-l|--verbose: Display console output on conversion.");
    println!("-v|--version: Display program version and exit.");
    println!("-h|--help: Display this help information and exit.");
    exit(code);
}

fn main() {
    let cli = CliOptions::new("cmongo2csv");
    let program = cli.get_program();

    let mut input = String::new();
    let mut output = String::new();
    let mut separator = ",".to_string();
    let mut tz = false;
    let mut mongo_types = true;
    let mut extensions = true;
    let mut verbose = false;

    if cli.get_num() > 1 {
        for (i, a) in cli.get_args().iter().enumerate() {
            match a.trim() {
                "-h" | "--help" => display_usage(&program, 0),
                "-v" | "--version" => display_version(),
                "-f" | "--file" => input = cli.next_argument(i),
                "-o" | "--out" => output = cli.next_argument(i),
                "-s" | "--separator" => separator = cli.next_argument(i),
                "-t" | "--tz" => tz = true,
                "-n" | "--no-mongo-types" => mongo_types = false,
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
        else output.is_empty() {
            display_error(&program, "No output file specified");
        }

        convert_json_to_csv(&input, &output, &separator, tz, mongo_types, verbose);
    }
    else {
        display_error(&program, "No options specified"); 
    }
}
