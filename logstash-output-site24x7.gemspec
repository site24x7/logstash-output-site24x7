Gem::Specification.new do |s|
  s.name          = 'logstash-output-site24x7'
  s.version       = '0.1.2'
  s.licenses      = ['']
  s.summary       = 'Site24x7 output plugin for Logstash event collector'
  s.homepage      = 'https://github.com/site24x7/logstash-output-site24x7'
  s.authors       = ['Magesh Rajan']
  s.email         = 'magesh.rajan@zohocorp.com'
  s.require_paths = ['lib']

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", "~> 2.0"
  s.add_runtime_dependency "logstash-codec-plain"
  s.add_runtime_dependency 'manticore', '>= 0.5.2', '< 1.0.0'
  s.add_runtime_dependency 'logstash-codec-json'

  s.add_development_dependency "logstash-devutils"
end
