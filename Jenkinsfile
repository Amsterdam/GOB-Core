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


node {
    stage("Checkout") {
        checkout scm
    }

    stage('Test') {
        tryStep "test", {
            sh "docker-compose -p gobcore -f gobcore/.jenkins/test/docker-compose.yml build && " +
               "docker-compose -p gobcore -f gobcore/.jenkins/test/docker-compose.yml run -u root --rm test"

        }, {
            sh "docker-compose -p gobcore -f gobcore/.jenkins/test/docker-compose.yml down"
        }
    }

}
