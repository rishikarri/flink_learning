# PyFlink Stateful Speed Tracker Example

This project is a sandbox for me to learn some of Flink's core concepts.

The `flink_speed_test.py` simulates a stream of car sensor data. It processes this stream to track the number of consecutive high-speed events (> 70) reported by each sensor individually. If a sensor reports 2 or more consecutive high speeds, it generates an alert.

### How to Run Locally (Apple Silicon Mac)

I had to run a few extra commands to get this working on my mac. Something to do with me having an M1 machine.

#### Setup & Installation

##### Step 1: Install Flink via Homebrew

This installs the x86_64 (Intel) Flink platform.

`brew install apache-flink`


##### Step 2: Install an Intel (x86_64) Java

The Intel Flink platform needs an Intel Java to run.

`arch -x86_64 brew install --cask temurin`


##### Step 3: Create a Pure Intel (x86_64) Python Environment

We will create a new virtual environment that is purely x86_64.

Start a new Intel (x86_64) shell:

`arch -x86_64 zsh`


(Your terminal is now emulating an Intel Mac).

Create a new venv (inside the Intel shell):
We'll call it .venv-intel.

`python3 -m venv .venv-intel`


Activate the new venv:

`source .venv-intel/bin/activate`


Upgrade pip and install Intel libraries:


`python3 -m pip install --upgrade pip
pip install "apache-flink==2.1.0" "numpy==1.24.4"`



### Running the Job

Every time you want to run your job, follow these steps.

Start Your Intel (x86_66) Shell:

`arch -x86_64 zsh`


Activate Your Intel Venv:
(Make sure to update this path if your project is in a different location)

source [your_project_path]/.venv-intel/bin/activate


Start the Flink Cluster:
The cluster must be running before you submit the job.

`$(brew --prefix apache-flink)/libexec/bin/start-cluster.sh`


You can verify it's running by opening http://localhost:8081 in your browser.

Run the PyFlink Job:
This command tells the Intel Flink cluster to use your Intel Python environment.
(Make sure to update these paths if your project is in a different location)

`flink run \
-py flink_speed_test.py \
-D python.client.executable=[your_project_path]/.venv-intel/bin/python \
-D python.executable=[your_project_path]/.venv-intel/bin/python`


You should see Job has been submitted... and Program execution finished.

How to See Your Output

The output does not go to your terminal. It goes to the Flink TaskManager log files.

Go to the Flink Web UI: http://localhost:8081

Click "Completed Jobs" on the left.

Click on your job (e.g., "StatefulFlinkExample").

Click the "Task Managers" tab.

There are two logs to check:

For ds.print() (Data Output): Click the stdout link. You will see the raw tuple data from your ds.print() and processed_stream.print() sinks.

For print(alert_msg) (Log Output): Click the .log link. Buried in the system logs, you will find your 🚨 ALERT: messages.

