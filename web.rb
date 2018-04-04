require 'nutcracker'
require 'nutcracker/web'
 
# Start nutcracker
nutcracker = Nutcracker.start(config_file: '/data/proxy/cluster.yaml')
#  
# Start the web service on port 1234 using Webrick
nutcracker.use(:web, Port: 1234)
#   
# Sleeping....
nutcracker.join
