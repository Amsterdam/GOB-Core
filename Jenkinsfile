#!groovy

def tryStep(String message, Closure block, Closure tearDown = null) {
    try {
        block()
    }
    catch (Throwable t) {
        slackSend message: "${env.JOB_NAME}: ${message} failure ${env.BUILD_URL}", channel: '#ci-channel', color: 'danger'

        throw t
    }
    finally {
        if (tearDown) {
            tearDown()
        }
    }
}


node('GOBBUILD') {
    stage("Checkout") {
        checkout scm
    }

    stage('Test') {
        lock("gob-core-test") {
            tryStep "test", {
                sh "docker-compose -p gobcore build && " +
                   "docker-compose -p gobcore run --rm test"

            }, {
                sh "docker-compose -p gobcore down"
            }
        }
    }

}
