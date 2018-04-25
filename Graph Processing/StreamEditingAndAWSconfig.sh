######################
### Stream Editing ###
######################

# Copy all files to single directory
for i in /Users/admin/workspace/Datasets/lastfm_train/*/*/*/*.json; do cp "$i" /Users/admin/workspace/Datasets/all_json; done

# Commands to merge files
zargs *.json -- cat | jq -s . > ../output.json
find . -name '*.json' -exec cat '{}' + | jq -s '.' > ../output.json
find . -name '*.json' -exec jq -s '{}' + > ../output.json

# Command to merge json output for visualisation
jq -s '.' *.json > output.json 

# Format for visualisation - json
# {
#     "nodes": [vertices.json],
#     "links": [edges.json]
# }


# Format for visualisation - GEXF - (not used)
# <gexf xmlns="http://www.gexf.net/1.2draft" version="1.2">
#     <graph mode="static" defaultedgetype="directed">
#         <nodes>
#             <node id="0" label="Hello" />
#             <node id="1" label="Word" />
#         </nodes>
#         <edges>
#             <edge id="0" source="0" target="1" />
#         </edges>
#     </graph>
# </gexf>

######################### 
### AWS Configuration ###
#########################

# AWS CLI configuration using IAM keys
aws configure

# ACCESS_KEY - not stored here
# SECRET_ACCESS_KEY - not stored here
# eu-west-1
# json

aws s3 ls - see buckets
aws s3 sync /Users/admin/workspace/Datasets/all.json s3://last-fm-data/json/all.json

# Log into cluster master node using EC2 key-pair - acquired from console
# Nessecary to add port access rule on 22 for ssh in appropriate security group using IAM console
ssh -i ~/myKey.pem hadoop@ec2-34-241-12-199.eu-west-1.compute.amazonaws.com


# command to start shell with graphframes package =>
spark-shell --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11