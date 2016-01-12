#
# Rakefile to build each tool via `cargo` and invoke it.
#

tools = ["ccsv2mongo", "cmongo2sql", "csql2mongo"]

task :default do
	for t in tools
		Dir.chdir(t) do
			puts "Building #{t}..."
			sh "cargo build --release"
		end
	end
	puts
end

task :test do
	for t in tools
		Dir.chdir(t) do 
			sh "target/release/#{t} --help"
			puts 
		end
	end
end
