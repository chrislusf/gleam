package ui

import (
	"text/template"
)

var JobStatusTpl = template.Must(template.New("job").Funcs(funcMap).Parse(`<!DOCTYPE html>
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

      {{ $start := .Status.Driver.StartTime }}

      <div class="row">
        <div class="col-sm-6">
          {{ with .Status.Driver }}
          <h2>{{ .Name }}</h2>
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
                <td>{{ unix .StartTime }}</td>
              </tr>
              {{ with .StopTime }}
              <tr>
                <th>Stop</th>
                <td>{{ unix . }}</td>
              </tr>
              {{ end}}
              <tr>
                <th>Duration</th>
                <td>{{ duration .StopTime $start}}</td>
              </tr>
            </tbody>
          </table>
          {{ end }}
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
        <div class="col-sm-6">
          <p>{{.Svg}}</p>
        </div>
        <div class="col-sm-6">
          {{ with .Status.Steps }}
            <h2>Steps</h2>
            <table class="table table-striped">
              <thead>
                <tr>
                  <th>Step</th>
                  <th>Name</th>
                </tr>
              </thead>
              <tbody>
              {{ range $step_index, $step := . }}
                <tr>
                  <td>{{ $step.Id }}</td>
                  <td>{{ $step.Name }}</td>
                </tr>
              {{ end }}
              </tbody>
            </table>
          {{ end }}
        </div>
      </div>

      {{ with .Status.TaskGroups }}
      <div class="row">
        <h2>Task Group</h2>
        <table class="table table-striped">
          <thead>
            <tr>
              <th>Steps</th>
              <th>Allocation</th>
              <th>Execution</th>
            </tr>
          </thead>
          <tbody>
          {{ range $tg_index, $tg := . }}
            <tr>
              <td>{{ $tg.StepIds }}</td>
              <td>
                {{with $tg.Allocation}}
                    {{.Allocated.MemoryMb}}MB {{.Location.DataCenter}}-{{.Location.Rack}}-{{.Location.Server}}:{{.Location.Port}}
                {{end}}
                <br/>
                
                {{with $tg.Request}}{{with .InstructionSet}}
                {{ range $inst_index, $inst := .Instructions }}
                    {{with .InputShardLocations}}Input: <ul>{{ range . }}<li>{{.Name}}@{{.Host}}:{{.Port}}</li>{{end}}</ul>{{end}}
                    {{with .OutputShardLocations}}Output:<ul>{{ range . }}<li>{{.Name}}@{{.Host}}:{{.Port}}</li>{{end}}{{end}}
                {{end}}
                {{ end }}{{ end }}
              </td>
              <td><ul>{{range .Executions}}
                   <li>
                     {{with .StartTime}} start: {{ duration . $start}} {{end}}
                     {{with .StopTime}} stop: {{ duration . $start}} {{end}}
                     {{with .ExecutionStat}}
                     <ul>
                       {{range .Stats}}
                          <li>{{.StepId}}:{{.TaskId}} {{.InputCounter}}=>{{.OutputCounter}}</li>
                       {{end}}
                     </ul>
                     {{end}}
                   </li>
                  {{end}}</ul></td>
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
