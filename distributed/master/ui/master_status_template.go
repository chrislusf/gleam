package ui

import (
	"text/template"
)

var MasterStatusTpl = template.Must(template.New("master").Funcs(funcMap).Parse(`<!DOCTYPE html>
<html>
  <head>
    <title>Gleam {{ .Version }}</title>
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css">
  </head>
  <body>
    <div class="container">
      <div class="page-header">
	    <h1>
          <a href="https://github.com/chrislusf/gleam">Gleam</a> <small>{{ .Version }}</small>
	    </h1>
      </div>

      <div class="row">
        <div class="col-sm-6">
          <h2>Cluster status</h2>
          <table class="table">
            <tbody>
              <tr>
                <th>Resource</th>
                <td>{{ .Topology.Resource }}</td>
              </tr>
              <tr>
                <th>Allocated</th>
                <td>{{ .Topology.Allocated }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div class="col-sm-6">
          <h2>System Stats</h2>
          <table class="table table-condensed table-striped">
            <tr>
              <th>Start Time</th>
              <td>{{ .StartTime }}</td>
            </tr>
            <tr>
              <th>Jobs Completed</th>
              <td><a href="/">{{.Logs.Len}}</a></td>
            </tr>
          </table>
        </div>
      </div>

      <div class="row">
        <h2>Topology</h2>
        <table class="table table-striped">
          <thead>
            <tr>
              <th>Data Center</th>
              <th>Rack</th>
              <th>Server</th>
              <th>Port</th>
              <th>Last Heartbeat</th>
              <th>Resource</th>
              <th>Allocated</th>
            </tr>
          </thead>
          <tbody>
          {{ range $dc_index, $dc := .Topology.DataCenters }}
            {{ range $rack_index, $rack := $dc.Racks }}
              {{ range $agent_index, $agent := $rack.Agents }}
            <tr>
              <td><code>{{ $dc.Name }}</code></td>
              <td>{{ $rack.Name }}</td>
              <td>{{ $agent.Location.Server }}</td>
              <td>{{ $agent.Location.Port }}</td>
              <td>{{ $agent.LastHeartBeat }}</td>
              <td>{{ $agent.Resource }}</td>
              <td>{{ $agent.Allocated }}</td>
            </tr>
              {{ end }}
            {{ end }}
          {{ end }}
          </tbody>
        </table>
      </div>

      <div class="row">
        <h2>Jobs</h2>
        <table class="table table-striped">
          <thead>
            <tr>
              <th>Id</th>
              <th>Name</th>
              <th>Driver</th>
              <th>User</th>
              <th>Host</th>
              <th>Duration</th>
            </tr>
          </thead>
          <tbody>
            {{ range $idx, $stat := $.Stats }}
            <tr>
              <td><a href="/job/{{$stat.Id}}">{{ $stat.Id }}</a></td>
              <td>{{ $stat.Driver.Name }}</td>
              <td>{{ $stat.Driver.Executable }}</td>
              <td>{{ $stat.Driver.Username }}</td>
              <td>{{ $stat.Driver.Hostname }}</td>
              <td>{{ duration $stat.Driver.StopTime $stat.Driver.StartTime }}</td>
            </tr>
            {{ end }}
          </tbody>
        </table>
      </div>

    </div>
  </body>
</html>
`))
