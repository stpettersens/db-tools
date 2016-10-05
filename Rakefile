#
# Rakefile to build each tool via `cargo` and invoke it.
#

require 'os'
require 'fileutils'

bin = "target/release/"

tools = [ "ccsv2mongo", "ccsv2sql", "cmongo2csv", "cmongo2sql", "csql2csv", "csql2mongo" ]

ins = [ "csv", "csv", "json", "json", "sql", "sql" ]
outs = [ "json", "sql", "csv", "sql", "csv", "json" ]

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
    for i in 0..tools.length - 1
        Dir.chdir(tools[i]) do
            sh "#{bin}#{tools[i]} --help"
            puts
            sh "#{bin}#{tools[i]} -f sample.#{ins[i]} -o out.#{outs[i]}"
            puts
            if OS.windows? then
                sh "type out.#{outs[i]}"
            else
                sh "cat out.#{outs[i]}"
            end
            puts
        end
    end
end
    
task :clean do
    for i in 0..tools.length - 1
        Dir.chdir(tools[i]) do
            FileUtils.rm_rf("target")
        end
    end
end
