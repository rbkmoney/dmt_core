#!groovy

build('dmt_core', 'docker-host') {

  checkoutRepo()

  runStage('compile') {
    withGithubSshCredentials {
      sh 'git submodule update --init'
    }
  }

  def pipeDefault
  def withWsCache
  runStage('load pipeline') {
    env.JENKINS_LIB = "builtils/jenkins_lib"
    pipeDefault = load("${env.JENKINS_LIB}/pipeDefault.groovy")
    withWsCache = load("${env.JENKINS_LIB}/withWsCache.groovy")
  }

  pipeDefault() {
    runStage('compile') {
      withGithubPrivkey {
        sh 'make wc_compile'
      }
    }
    runStage('lint') {
      sh 'make wc_lint'
    }
    runStage('xref') {
      sh 'make wc_xref'
    }
    runStage('dialyze') {
      withWsCache("_build/default/rebar3_22.3.1_plt") {
        sh 'make wc_dialyze'
      }
    }
    runStage('test') {
      sh 'make wc_test'
    }
  }
}
