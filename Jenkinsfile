pipeline {
    agent {
        docker {
            image 'maven:3.3-jdk-8'
            args '-v /root/.m2:/root/.m2'
        }
    }
    stages {
        stage('Build') {
            steps {
		        echo 'Building the application...'
                sh 'mvn clean compile'
            }
        }
        stage('Test') {
            steps {
		        echo 'Testing the application...'
                sh 'mvn test' 
            }
            post {
                always {
                    junit 'target/surefire-reports/*.xml' 
                }
            }
        }
	    stage('Deploy') {
	        steps {
		        echo 'Deploying the application...'
	        }
        }
    }
}
