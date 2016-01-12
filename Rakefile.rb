#
# Rakefile to build each tool via `cargo` and invoke it.
#

tools = ["ccsv2mongo", "cmongo2sql", "csql2mongo"]

task :default do
	for t in tools
		puts
		Dir.chdir(t) do
			puts "Building #{t}..."
			sh "cargo build --release"
			sh "target/release/#{t} --help"
		end
	end
	puts
end

task :test do
	puts "!TODO"
end
