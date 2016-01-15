/*
ccsv2mongo
Utility to convert a CSV file to a MongoDB JSON dump.

Copyright 2016 Sam Saint-Pettersen.
Licensed under the MIT/X11 License.

Rust port of original Python tool (1.0.2).
*/

extern crate clioptions;
extern crate regex;
extern crate csv;
use clioptions::CliOptions;
use regex::Regex;
use std::io::{BufRead, BufReader, Write};
use std::fs::File;
use std::process::exit;

/*fn is_date(v: &str) -> bool {
    let re = Regex::new(r"\d{4}-\d{2}-\d{2}.*").unwrap();
    let mut date = false;
    if re.is_match(&v) {
        date = true;
    }
    date
}*/

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

fn convert_csv_to_json(input: &str, output: &str, separator: &str, 
tz: bool, mongo_types: bool, array: bool, verbose: bool) {
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

    let mut processed: Vec<Vec<String>> = Vec::new();
    for r in records {
        let mut record: Vec<String> = Vec::new();
        for f in r {
            // ObjectIds
            let mut re = Regex::new("ObjectId((.*))").unwrap();
            if re.is_match(&f) {
                for cap in re.captures_iter(&f) {
                    let mut field = cap.at(1).unwrap().to_string();
                    if mongo_types {
                        field = format!("{{\"$oid\":\"{}\"}}", &field[1..field.len() - 1]);
                    }
                    else {
                        field = format!("\"{}\"", &field[1..field.len() - 1]);
                    }
                    record.push(field);
                }
                continue;
            }
            // Dates 
            re = Regex::new(r"(\d{4}-\d{2}-\d{2}.*)").unwrap();
            if re.is_match(&f) {
                for cap in re.captures_iter(&f) {
                    let mut field = cap.at(1).unwrap().to_string();
                    if mongo_types {
                        field = format!("{{\"$date\":\"{}\"}}", parse_timestamp(&field[0..field.len()], tz));
                    }
                    else {
                        field = format!("\"{}\"", field);
                    }
                    record.push(field);
                }
                continue;
            }
            // Numbers
            re = Regex::new(r"\d+.*").unwrap();
            if re.is_match(&f) {
                let n = f.parse::<f32>().ok();
                let v = match n {
                    Some(v) => v,
                    None => 0 as f32
                };
                record.push(format!("{:.*}", 2, v));
                continue;
            }
            // Booleans
            re = Regex::new("TRUE|true|FALSE|false").unwrap();
            if re.is_match(&f) {
                record.push(f.to_string().to_lowercase());
                continue;
            }
            // Strings
            re = Regex::new(r"\w+").unwrap();
            if re.is_match(&f) {
                /*if is_date(&f) {
                    record.push(format!("\"{}\"", parse_timestamp(&f, tz)));
                }*/
                record.push(format!("\"{}\"", f));
                continue;
            }
        }
        processed.push(record);
    }

    let mut json: Vec<String> = Vec::new();
    let v = &processed;
    for record in v.chunks(headers.len()) {
        let no_comma = record.len() - 1;
        for (i, r) in record.iter().enumerate() {
            let mut ff: Vec<String> = Vec::new();
            for (x, f) in r.iter().enumerate() {
                ff.push(format!("\"{}\":{}", headers[x], f));
            }
            let mut fr = format!("{{{}}}", ff.join(","));
            if i < no_comma && array {
                fr = format!("{},", fr);
            }
            json.push(fr);  
        }
    }

    if array {
        json.insert(0, "[".to_string());
        json.push("]".to_string());
    }

    json.push(String::new());

    if verbose {
        println!("Generating MongoDB JSON dump file: '{}' from", output);
        println!("CSV file: '{}'.\n", input);
    }

    let mut w = File::create(output).unwrap();
    let _ = w.write_all(json.join("\n").as_bytes());
}

fn check_extensions(program: &str, input: &str, output: &str) {
    let mut re = Regex::new(r".csv$").unwrap();
    if !re.is_match(&input) {
        display_error(&program, &format!("Input file '{}' is not CSV", &input));
    }
    re = Regex::new(r".json$").unwrap();
    if !re.is_match(&output) {
        display_error(&program, &format!("Output file '{}' is not JSON", &output));
    }
}

fn display_error(program: &str, err: &str) {
    println!("Error: {}.", err);
    display_usage(&program, -1);
}

fn display_version() {
    let signature = "ccsv2mongo 1.0.0 (https://github.com/stpettersens/db-tools)";
    println!("{}", signature);
    exit(0);
}

fn display_usage(program: &str, code: i32) {
    println!("\nccsv2mongo");
    println!("Utility to convert a CSV file to a MongoDB JSON dump.");
    println!("\nCopyright 2016 Sam Saint-Pettersen.");
    println!("Licensed under the MIT/X11 License.");
    println!("\nUsage: {} -f|--file <input.csv> -o|--out <output.sql> -s|--separator <separator>", program);
    println!("-n|--no-mongo-types -a|--array -i|--ignore-ext -l|--verbose [-v|--version][-h|--help]");
    println!("\n-f|--file: CSV file to convert.");
    println!("-o|--out: MongoDB JSON file as output.");
    println!("-s|--separator: Set field seperator (default: ,).");
    println!("-t|--tz: Use \"Z\" as timezone for timestamps rather than +0000");
    println!("-n|--no-mongo-types: Do not use MongoDB types in output.");
    println!("-a|--array: Output MongoDB records as a JSON array.");
    println!("-i|--ignore-ext: Ignore file extensions for input/output.");
    println!("-l|--verbose: Display console output on conversion.");
    println!("-v|--version: Display program version and exit.");
    println!("-h|--help: Display this help information and exit.");
    exit(code);
}

fn main() {
    
    let cli = CliOptions::new("ccsv2mongo");
    let program = cli.get_program();

    let mut input = String::new();
    let mut output = String::new();
    let mut separator = ",".to_string();
    let mut tz = false;
    let mut mongo_types = true;
    let mut array = false;
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
                "-a" | "--array" => array = true,
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

        convert_csv_to_json(&input, &output, &separator, tz, mongo_types, array, verbose);
    }
    else {
        display_error(&program, "No options specified"); 
    }
}
