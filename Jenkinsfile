#!groovy

build('dmt_core', 'docker-host') {
  checkoutRepo()
  loadBuildUtils("builtils")

  def pipeErlangLib
  runStage('load pipeline') {
    env.JENKINS_LIB = "builtils/jenkins_lib"
    env.SH_TOOLS = "builtils/sh"
    pipeErlangLib = load("${env.JENKINS_LIB}/pipeErlangLib.groovy")
  }

  pipeErlangLib.runPipe(false, true, 'test')
}
