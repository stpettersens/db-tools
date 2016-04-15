fn main() {
	let foo = "Samuel";
	let mut last = 'a';
	for _char in foo.chars() {
		println!("{} = ASCII {}", _char, _char as u8);
		last = _char;
	}
	println!("The last character of {} is {} ({})", foo, last, last as u8);
}
