# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'scalastic/version'

Gem::Specification.new do |spec|
  spec.name          = "scalastic"
  spec.version       = Scalastic::VERSION
  spec.authors       = ["Aliaksei Baturytski"]
  spec.email         = ["abaturytski@gmail.com"]

  spec.summary       = "Elasticsearch document partitions"
  spec.description   = "Elasticsearch alias-based partitions for scalable indexing and searching"
  spec.homepage      = "https://github.com/aliakb/scalastic"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.10"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.4"
  spec.add_development_dependency "simplecov", "~> 0.11"

  spec.add_dependency "elasticsearch", "~> 1.0"
end
