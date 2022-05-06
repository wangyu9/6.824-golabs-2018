# 6.824-golabs-2018

This repo is the semester-long project of Yu Wang for the graduate level course 6.824 Distributed Systems Engineering 
(http://nil.csail.mit.edu/6.824/2018/) done in Spring 2018. 

# Sync with instructor code

The code was imported from git://g.csail.mit.edu/6.824-golabs-2018

I followed the instruction here to add another pull from source:
https://stackoverflow.com/questions/849308/pull-push-from-multiple-remote-locations

By issuing
`git remote add alt git://g.csail.mit.edu/6.824-golabs-2018`

So now issuing
`git pull alt master`
(and doing git commit and push)

Allows me to keep updated to git://g.csail.mit.edu/6.824-golabs-2018

To test it on the atlas, do the following:

`git config credential.helper store `
to save username and password.

`add git`

`setup ggo_v1.9`

`git clone https://github.com/wangyu9/6.824-golabs-2018.git`

`git pull`

`cd 6.824-golabs-2018`

`export "GOPATH=$PWD"`

`cd ./src/raft`
