package ui

import (
	"html/template"
)

var JobStatusTpl = template.Must(template.New("job").Parse(`<!DOCTYPE html>
<html>
  <head>
    <title>Job {{ .Status.Id }}</title>
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
          {{ with .Status.Driver }}
          <h2>Driver Program</h2>
          <table class="table">
            <tbody>
              <tr>
                <th>User</th>
                <td>{{ .Username }}</td>
              </tr>
              <tr>
                <th>Host</th>
                <td>{{ .Hostname }}</td>
              </tr>
              <tr>
                <th>Executable</th>
                <td style="max-width:150px;word-wrap:break-word;">{{ .Executable }}</td>
              </tr>
              <tr>
                <th>Start</th>
                <td>{{ .StartTime }}</td>
              </tr>
              <tr>
                <th>Stop</th>
                <td>{{ .StopTime }}</td>
              </tr>
            </tbody>
          </table>
          {{ end }}
        </div>

        <div class="col-sm-6">
          <h2>System Stats</h2>
          <table class="table table-condensed table-striped">
            <tr>
              <th>Jobs Completed</th>
              <td>100</td>
            </tr>
          </table>
        </div>
      </div>

      {{ with .Status.TaskGroups }}
      <div class="row">
        <h2>Task Group</h2>
        <table class="table table-striped">
          <thead>
            <tr>
              <th>Steps</th>
              <th>Tasks</th>
              <th>Name</th>
              <th>Allocation</th>
              <th>CPU</th>
              <th>Memory</th>
            </tr>
          </thead>
          <tbody>
          {{ range $tg_index, $tg := . }}
            <tr>
              <td>{{ $tg.StepIds }}</td>
              <td>{{ $tg.TaskIds }}</td>
              <td>{{with $tg.Request}}{{.}}{{end}}</td>
              <td>{{with $tg.Allocation}}{{.}}{{end}}</td>
              <td>{{with $tg.Request}}{{.Resource.CpuCount}}{{end}}</td>
              <td>{{with $tg.Request}}{{.Resource.MemoryMb}}{{end}}</td>
            </tr>
          {{ end }}
          </tbody>
        </table>
      </div>
      {{ end }}

    </div>
  </body>
</html>
`))
