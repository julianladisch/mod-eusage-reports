# Updating the `mod-eusage-reports` module for the Thor project in Rancher

Adam's terse notes taken from
[a thread on Slack](https://folio-project.slack.com/archives/C027NNFMT2S/p1626772031005900)
[[screenshot]](updating-mod-eusage-reports-on-thor-server.png)

* Deployment on Thor instructions... I'll post it here.. Hope that's ok @julianladisch.
* https://jenkins-aws.indexdata.com/job/folio-org/job/mod-eusage-reports/job/master/
* We see that latest master it .40. You probably already know to inspect this on Jenkins.
* Go to https://rancher.dev.folio.org/g/clusters
* You probably will be asked to enter GitHub account..
* After that point. Select "Global", "folio-eks-2", "thor"
* Go down that list and find the mod-eusage-reports entry. Click on the last column  with 3 vertical dots.. A popup menu will appear .. in there select "View/Edit YAML"
* That will open an editor in the browser.. Scroll down a bit until you find "image: folioci/mod-eusage-reports:0.1.0-SNAPSHOT.39"
* Change the 39 to 40. And hit "Save".
* You are done.. Good!
* That was deployment.. Next is enable.
* See https://dev.folio.org/faqs/how-to-get-started-with-rancher/
* Go down to section "Registering modules in Okapi"
* The first method there is the one to use.
