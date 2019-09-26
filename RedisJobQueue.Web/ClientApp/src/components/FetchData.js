import React, { Component } from 'react';

export class FetchData extends Component {
  static displayName = FetchData.name;

  constructor (props) {
    super(props);
    this.state = { jobs: [], runs : [], loading: true };
  }
  
  async componentDidMount() {
     await this.getJobList();
  }
  
  async getJobList() {
      const response = await fetch('api/Job/Jobs');
      const data = await response.json();
      this.setState({ jobs: data, loading: false });
  }
  
  async getRuns(job) {
      const response = await fetch(`api/Job/Runs?job=${job}`);
      const data = await response.json();
      console.log(data);
      this.setState({ runs: data });
  }

  renderTable () {
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
  
  renderRuns () {
      return (
          <>
              <h1>Runs</h1>
              <table className='table table-striped'>
                  <thead>
                  <tr>
                      <th>Name</th>
                      <th>Machine ID</th>
                      <th>Machine Name</th>
                      <th>Date</th>
                  </tr>
                  </thead>
                  <tbody>
                  {this.state.runs.map(run =>
                      <tr key={run.name}>
                          <td>{run.name}</td>
                          <td>{run.machineId}</td>
                          <td>{run.machineName.replace('Jonathans-', 'MadDev-')}</td>
                          <td>{run.timestamp}</td>
                      </tr>
                  )}
                  </tbody>
              </table>
          </>
      );
  }

  render () {
      
    let contents = this.state.loading
      ? <p><em>Loading...</em></p>
      : this.renderTable();

    return (
      <div>
        <h1>Jobs</h1>
        {contents}
        {this.state.runs.length > 0 && this.renderRuns()}
      </div>
    );
  }
}
