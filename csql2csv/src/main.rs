/*
csql2csv
Utility to convert a SQL dump to a CSV file.

Copyright 2016 Sam Saint-Pettersen.
Licensed under the MIT/X11 License.

Rust port of original Python tool (1.0.0).
*/

extern crate clioptions;
extern crate regex;
use clioptions::CliOptions;
use regex::Regex;
use std::io::{BufRead, BufReader, Write};
use std::fs::File;
use std::process::exit;

fn preprocess_sql(lines: Vec<String>) -> Vec<String> {
    let mut processed: Vec<String> = Vec::new();
    let patterns = vec![
        "VALUES \\(",
        ",",
        "\\),",
        "\\(",
        "\n\n",
    ];

    let repls = vec![
        "VALUE (\n",
        ",\n",
        "\nINSERT INTO `null` VALUES (\n",
        "",
        "\n",
    ];

    for line in lines {
        let mut i: usize = 0;
        let mut l = String::new();
        for p in patterns.clone() {
            let re = Regex::new(p).unwrap();
            l = re.replace(&line, repls[i]);    
            i += 1;
        }
        if l.len() > 0 {
            processed.push(l);
        }
    }
    processed
}

fn convert_sql_to_csv(input: &str, output: &str, separator: &str, tz: bool, verbose: bool) {
    let mut lines: Vec<String> = Vec::new();
    let f = File::open(input).unwrap();
    let file = BufReader::new(&f);
    for line in file.lines() {
        lines.push(line.unwrap());
    }

    let processed = preprocess_sql(lines);
    
    let mut fields: Vec<String> = Vec::new();
    let mut values: Vec<String> = Vec::new();
    let mut inserts: Vec<Vec<String>> = Vec::new();
    let mut headers = false;
    for line in processed {
        let mut re = Regex::new("CREATE TABLE|UNLOCK TABLES").unwrap();
        if re.is_match(&line) {
            headers = true;
        }
        re = Regex::new(r"(^[`a-zA-Z0-9_]+)").unwrap();
        if headers {
            for cap in re.captures_iter(&line) {
                let f = cap.at(1).unwrap();
                let mut re = Regex::new("`").unwrap();
                let mut f = re.replace(&f, "");
                re = Regex::new("CREATE|ENGINE|INSERT|PRIMARY|LOCK").unwrap();
                f = re.replace(&f, "");
                if f.len() > 0 {
                    fields.push(f);
                }
            }
        }
        re = Regex::new("INSERT INTO").unwrap();
        if re.is_match(&line) {
            headers = false;
        }
        re = Regex::new(r"(^[\d.]+)").unwrap();
        if !headers {
            for cap in re.captures_iter(&line) {
                values.push(cap.at(1).unwrap().to_string());
            }
        }
        re = Regex::new(r"'([\w\s]+)'").unwrap();
        if !headers {
            for cap in re.captures_iter(&line) {
                values.push(format!("{}", cap.at(1).unwrap().to_string()));
            }
        }
        re = Regex::new("(TRUE|FALSE|NULL)").unwrap();
        if !headers {
            for cap in re.captures_iter(&line) {
                values.push(cap.at(1).unwrap().to_string().to_lowercase());
            }
        }
        re = Regex::new(r"'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})'").unwrap();
        if !headers {
            for cap in re.captures_iter(&line) {
                let mut v = cap.at(1).unwrap().to_string();
                let re = Regex::new(r"\s").unwrap();
                v = re.replace(&v, "T");
                v = format!("{}.000", v);
                if tz {
                    v = format!("{}Z", v);
                }
                else {
                    v = format!("{}+0000", v);
                }
                values.push(v);
            }
        }
    } 

    let v = &values;
    for record in v.chunks(fields.len()) {
        let mut formatted: Vec<String> = Vec::new();
        for element in record {
        	formatted.push(element.to_string());
        }
        inserts.push(formatted);
    }

    let mut csv: Vec<String> = Vec::new();
    csv.push(fields.join(separator));
    for record in inserts {
    	csv.push(record.join(separator));
    }
    
    csv.push(String::new());

    if verbose {
        println!("Generating CSV file: '{}' from", output);
        println!("SQL dump file: '{}'.\n", input);
    }

    let mut w = File::create(output).unwrap();
    let _ = w.write_all(csv.join("\n").as_bytes());
}

fn check_extensions(program: &str, input: &str, output: &str) {
    let mut re = Regex::new(r".sql$").unwrap();
    if !re.is_match(&input) {
        display_error(&program, &format!("Input file '{}' is not SQL", &input));
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
    let signature = "csql2csv 1.0.0 (https://github.com/stpettersens/db-tools)";
    println!("{}", signature);
    exit(0);
}

fn display_usage(program: &str, code: i32) {
    println!("\ncsql2csv");
    println!("Utility to convert a SQL dump to a CSV file.");
    println!("\nCopyright 2016 Sam Saint-Pettersen.");
    println!("Licensed under the MIT/X11 License.");
    println!("\nUsage: {} -f|--file <input.sql> -o|--out <output.json>", program);
    println!("-t|--tz -n|--no-mongo-types -a|--array -i|--ignore-ext -l|--verbose [-v|--version][-h|--help]");
    println!("\n-f|--file: SQL file to convert.");
    println!("-o|--out: MongoDB JSON file as output.");
    println!("-s|--separator: Separator to use for output (default: ,).");
    println!("-t|--tz: Use \"Z\" as timezone for timestamps rather than +0000");
    println!("-i|--ignore-ext: Ignore file extensions for input/output.");
    println!("-l|--verbose: Display console output on conversion.");
    println!("-v|--version: Display program version and exit.");
    println!("-h|--help: Display this help information and exit.");
    exit(code);
}

fn main() {
	let cli = CliOptions::new("csql2mongo");
    let program = cli.get_program();
    
    let mut input = String::new();
    let mut output = String::new();
    let mut separator = ",".to_string();
    let mut tz = false;
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

        convert_sql_to_csv(&input, &output, &separator, tz, verbose);

    }
    else {
        display_error(&program, "No options specified"); 
    }
}
