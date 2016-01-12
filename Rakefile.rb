#
# Rakefile to build each tool via `cargo` and invoke it.
#

bin = "target/release/"

tools = ["ccsv2mongo", "cmongo2sql", "csql2mongo"]

ins = [ "csv", "json", "sql" ]
outs = [ "json", "sql", "json" ]

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
    i = 0
	for t in tools do
		Dir.chdir(t) do 
			sh "#{bin}#{t} --help"
			puts 
            sh "#{bin}#{t} -f sample.#{ins[i]} -o out.#{outs[i]}"
            puts
            sh "cat out.#{outs[i]}"
            puts
		end
        i += 1
	end
end
