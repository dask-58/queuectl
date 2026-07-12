package dashboard

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="2">
    <title>QueueCTL Dashboard</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background-color: #f9f9f9;
            color: #333;
            margin: 0;
            padding: 20px;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        h1, h2 {
            border-bottom: 1px solid #ddd;
            padding-bottom: 10px;
        }
        .stats {
            display: flex;
            gap: 20px;
            margin-bottom: 40px;
            flex-wrap: wrap;
        }
        .stat-card {
            background: white;
            border: 1px solid #ddd;
            border-radius: 6px;
            padding: 20px;
            min-width: 120px;
            text-align: center;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }
        .stat-value {
            font-size: 2em;
            font-weight: bold;
            margin-bottom: 5px;
        }
        .stat-label {
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
            border: 1px solid #ddd;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }
        th, td {
            text-align: left;
            padding: 12px;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #f1f1f1;
            font-weight: 600;
        }
        .state-pending { color: #856404; }
        .state-processing { color: #004085; }
        .state-completed { color: #155724; }
        .state-failed, .state-dead { color: #721c24; }
    </style>
</head>
<body>
    <div class="container">
        <h1>QueueCTL Dashboard</h1>
        
        <h2>Queue Statistics</h2>
        <div class="stats">
            <div class="stat-card">
                <div class="stat-value">{{.Status.PendingJobs}}</div>
                <div class="stat-label">Pending</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{.Status.ProcessingJobs}}</div>
                <div class="stat-label">Processing</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{.Status.CompletedJobs}}</div>
                <div class="stat-label">Completed</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{.Status.FailedJobs}}</div>
                <div class="stat-label">Failed</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{.Status.DeadJobs}}</div>
                <div class="stat-label">Dead</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{.Status.ActiveWorkers}}</div>
                <div class="stat-label">Workers</div>
            </div>
        </div>

        <h2>Recent Jobs</h2>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>STATE</th>
                    <th>ATTEMPTS</th>
                    <th>COMMAND</th>
                </tr>
            </thead>
            <tbody>
                {{range .Jobs}}
                <tr>
                    <td>{{.ID}}</td>
                    <td class="state-{{.State}}">{{.State}}</td>
                    <td>{{.Attempts}}</td>
                    <td><code>{{.Command}}</code></td>
                </tr>
                {{else}}
                <tr>
                    <td colspan="4">No recent jobs</td>
                </tr>
                {{end}}
            </tbody>
        </table>
    </div>
</body>
</html>`
