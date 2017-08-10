package ui

import (
	"bytes"
	"fmt"

	"github.com/ajstarks/svgo"
	"github.com/chrislusf/gleam/pb"
)

var (
	WidthStep       = 20 * m
	HightStep       = 3 * m
	HightStepHeader = 2 * m
	Margin          = 5 * m
	LineLength      = 5 * m
	SmallMargin     = 4
	VerticalGap     = 4 * m
)

type stepGroupPosition struct {
	input  point
	output point
}

func GenSvg(status *pb.FlowExecutionStatus) string {

	var svgWriter bytes.Buffer
	canvas := svg.New(&svgWriter)

	width, height := doFlowExecutionStatus(canvas, status, 100*m)
	height += Margin

	svgWriter.Truncate(0)

	canvas.Start(width, height)
	doFlowExecutionStatus(canvas, status, width)
	canvas.End()

	return svgWriter.String()
}

func doFlowExecutionStatus(canvas *svg.SVG, status *pb.FlowExecutionStatus, width int) (largestWidth, height int) {

	positions := make([]stepGroupPosition, len(status.GetStepGroups()))
	layerOfStepGroupIds := toStepGroupLayers(status)

	stepGroupOutPoint := doState(canvas, point{width / 2, Margin}, "start")

	height = stepGroupOutPoint.y
	var lastOutputs []point

	for layer := len(layerOfStepGroupIds) - 1; layer >= 0; layer-- {
		stepGroupIds := layerOfStepGroupIds[layer]

		// determine the largest width
		w := len(stepGroupIds)*(VerticalGap+WidthStep) + VerticalGap
		if largestWidth < w {
			largestWidth = w
		}

		// determine input points
		for idx, stepGroupId := range stepGroupIds {
			positions[stepGroupId].input = point{
				width/2 + (2*idx-(len(stepGroupIds)-1))*(WidthStep+VerticalGap)/2,
				height + LineLength,
			}
		}

		lastOutputs = make([]point, 0)

		for _, stepGroupId := range stepGroupIds {
			stepGroup := status.StepGroups[stepGroupId]

			if len(stepGroup.ParentIds) == 0 {
				connect(canvas, stepGroupOutPoint, positions[stepGroupId].input, "input")
			}

			for idx, parentId := range stepGroup.ParentIds {
				acceptor := point{
					positions[stepGroupId].input.x + (2*idx-(len(stepGroup.ParentIds)-1))*10*m/2,
					positions[stepGroupId].input.y,
				}
				lastStepId := getLastStepId(status, status.StepGroups[parentId])
				outputDatasetId := status.Steps[lastStepId].OutputDatasetId
				outputDatasetSize := collectStepOutputDatasetSize(status, lastStepId)
				connect(canvas, positions[parentId].output, acceptor,
					fmt.Sprintf("d%d %d", outputDatasetId, outputDatasetSize))
			}

			positions[stepGroupId].output = doStepGroup(canvas, positions[stepGroupId].input, status, stepGroup)
			lastOutputs = append(lastOutputs, positions[stepGroupId].output)

			if positions[stepGroupId].output.y > height {
				height = positions[stepGroupId].output.y
			}

		}
	}

	endInPoint := point{width / 2, height + LineLength}
	doState(canvas, endInPoint, "end")
	for _, p := range lastOutputs {
		connect(canvas, p, endInPoint, "output")
	}

	height = endInPoint.y + Margin

	return largestWidth, height
}

func doState(canvas *svg.SVG, input point, state string) (output point) {
	r := 3 * m
	canvas.Circle(input.x, input.y+r, r)
	canvas.Text(input.x, input.y+r+0.5*m, state, "text-anchor:middle;font-size:20px;fill:white")
	return point{input.x, input.y + 2*r}
}

func doStepGroup(canvas *svg.SVG, input point, status *pb.FlowExecutionStatus, stepGroup *pb.FlowExecutionStatus_StepGroup) (output point) {
	x, y := input.x-WidthStep/2, input.y
	w, h := WidthStep, len(stepGroup.GetStepIds())*(HightStep+SmallMargin)+SmallMargin

	stepOut := point{input.x, input.y}
	for _, stepId := range stepGroup.StepIds {
		step := status.GetStep(stepId)
		stepOut = doStep(canvas, stepOut, step, isStepGroupFinished(status, stepGroup))
	}

	rectstyle := fmt.Sprintf("stroke:%s;stroke-width:1;fill:%s", "black", "none")

	canvas.Rect(x, y, w, h, rectstyle)

	output.x = input.x
	output.y = input.y + h

	return
}

func doStep(canvas *svg.SVG, input point, step *pb.FlowExecutionStatus_Step, hasFinished bool) (output point) {
	output.x = input.x
	output.y = input.y + HightStep + SmallMargin

	x, y := input.x-WidthStep/2+SmallMargin, input.y+SmallMargin
	w, h := WidthStep-2*SmallMargin, HightStep
	fs := 14
	w2 := 3

	color := "#fff"
	if hasFinished {
		color = "#D7DBDD"
	}

	name := fmt.Sprintf("%d.%s", step.Id, step.Name)
	canvas.Rect(x, y, w, h, fmt.Sprintf("stroke:%s;stroke-width:1;fill:%s", "black", color))
	canvas.Gstyle(fmt.Sprintf("font-size:%dpx", fs))
	canvas.Text(x+w2, y+HightStep-SmallMargin/2, name, "baseline-shift:50%")
	canvas.Gend()

	return
}

func getLastStepId(status *pb.FlowExecutionStatus, stepGroup *pb.FlowExecutionStatus_StepGroup) int32 {
	return stepGroup.StepIds[len(stepGroup.StepIds)-1]
}

func isStepGroupFinished(status *pb.FlowExecutionStatus, stepGroup *pb.FlowExecutionStatus_StepGroup) bool {
	isFinished := true
	for _, tg := range status.TaskGroups {
		if tg.StepIds[0] == stepGroup.StepIds[0] {
			isThisTaskGroupFinished := false
			for _, exe := range tg.Executions {
				if exe.StopTime != 0 {
					isThisTaskGroupFinished = true
				}
			}
			if !isThisTaskGroupFinished {
				isFinished = false
			}
		}
	}
	return isFinished
}

// may be more efficient to map stepId=>size
func collectStepOutputDatasetSize(status *pb.FlowExecutionStatus, stepId int32) (counter int64) {
	for _, tg := range status.TaskGroups {
		for _, execution := range tg.Executions {
			if execution.ExecutionStat == nil {
				continue
			}
			for _, stat := range execution.ExecutionStat.Stats {
				if stat.StepId == stepId {
					counter += stat.OutputCounter
				}
			}
		}
	}
	return counter
}
