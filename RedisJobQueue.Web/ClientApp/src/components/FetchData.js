import React, {Component} from 'react';

export class FetchData extends Component {
    static displayName = FetchData.name;

    runsInterval = null;
    enqueuedInterval = null;

    constructor(props) {
        super(props);
        this.state = {jobs: [], runs: [], count : 0, selectedRun: -1, selectedJob : '', loading: true};
    }

    async componentDidMount() {
        this.getEnqueuedCount();
        await this.getJobList();
    }

    async getJobList() {
        const response = await fetch('api/Job/Jobs');
        const data = await response.json();
        this.setState({jobs: data, loading: false});
    }
    
    async getEnqueuedCount() {
        const execute = async () => {
            const response = await fetch(`api/Job/EnqueuedCount`);
            const data = await response.json();
            this.setState({count: data});
        };
        if (this.enqueuedInterval) {
            clearInterval(this.enqueuedInterval);
            this.enqueuedInterval = null;
        }
        this.enqueuedInterval = setInterval(execute, 1000);
        await execute();
    }

    async getRuns(job) {
        const execute = async () => {
            this.setState({selectedJob : job});
            const response = await fetch(`api/Job/Runs?job=${job}`);
            const data = await response.json();
            this.setState({runs: data});
        };
        if (this.runsInterval) {
            clearInterval(this.runsInterval);
            this.runsInterval = null;
        }
        this.runsInterval = setInterval(execute, 500);
        await execute();
    }


    async enqueue(job) {
       const count = window.prompt("How many to enqueue?", "1");
       await fetch(`api/Job/Enqueue?name=${job}&count=` + parseInt(count));
    }

    renderTable() {
        return (
            <table className='table table-striped'>
                <thead>
                <tr>
                    <th>Name</th>
                </tr>
                </thead>
                <tbody>
                {this.state.jobs.map(name =>
                    <tr key={name} onClick={() => this.getRuns(name)}>
                        <td>{name}</td>
                    </tr>
                )}
                </tbody>
            </table>
        );
    }

    statusColor = (status) => {
        switch (status) {
            case 'Errored':
                return '#f36060';
            case 'Running':
                return '#5ab2ff';
            case 'Success':
                return 'rgb(90, 255, 105)'
            case 'Retrying':
                return 'rgb(255,78,191)'
        }
    };

    statusMessage = (run) => {
        if (run.status === 'Retrying') {
            return `${run.status} (${run.retries})`
        }
        return run.status;
    };

    renderRunDetails() {
        const run = this.state.runs[this.state.selectedRun];
        return <div>
            <h1>{run.name} Details - {run.timestamp}</h1>
            <p>Status: <span style={{color : this.statusColor(run.status)}}>{this.statusMessage(run)}</span></p>
            <p>Exception: <span style={{color : '#f36060'}}>{run.exception}</span></p>
        </div>
    };

    renderRuns() {
        return (
            <>
                <h1>Runs For {this.state.selectedJob}</h1>
                <a href={"#"} onClick={() => this.enqueue(this.state.selectedJob)}>Enqueue Now</a>
                <table className='table table-striped'>
                    <thead>
                    <tr>
                        <th>Name</th>
                        <th>Machine ID</th>
                        <th>Date</th>
                        <th>Status</th>
                        <th>Actions</th>
                    </tr>
                    </thead>
                    <tbody>
                    {this.state.runs.map((run, index) =>
                        <tr key={run.runId}>
                            <td>{run.name}</td>
                            <td>{run.machineId}</td>
                            <td>{run.machineName}</td>
                            <td>{run.timestamp}</td>
                            <td style={{backgroundColor: this.statusColor(run.status)}}>{this.statusMessage(run)}</td>
                            <td>
                                <a href={"#"} onClick={() => this.setState({selectedRun: index})}>Details</a>
                                <a href={"#"} onClick={() => this.enqueue(run.name)}>Enqueue</a>
                            </td>
                        </tr>
                    )}
                    </tbody>
                </table>
            </>
        );
    }

    render() {

        let contents = this.state.loading
            ? <p><em>Loading...</em></p>
            : this.renderTable();

        return (
            <div>
                <h1>Jobs (Enqueued: {this.state.count})</h1>
                {contents}
                {this.state.selectedRun !== -1 && this.state.runs.length > 0 && this.renderRunDetails()}
                {this.state.selectedJob && this.renderRuns()}
            </div>
        );
    }
}
