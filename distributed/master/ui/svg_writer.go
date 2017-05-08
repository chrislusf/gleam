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
)

func GenSvg(status *pb.FlowExecutionStatus) string {

	var svgWriter bytes.Buffer
	canvas := svg.New(&svgWriter)

	width := 100 * m
	height := Margin + HightStep + LineLength // start
	for _, stepGroup := range status.StepGroups {
		height += len(stepGroup.GetStepIds()) * HightStep
		height += LineLength + HightStepHeader
	}
	height += Margin // end

	canvas.Start(width, height)

	startOutPoint := doState(canvas, point{width / 2, Margin}, "start")

	stepGroupOutPoint := point{startOutPoint.x, startOutPoint.y + LineLength}
	prevSourcePoint := startOutPoint

	for _, stepGroup := range status.StepGroups {

		inputDatasets := getStepInputDatasets(status, status.GetStep(stepGroup.StepIds[0]))

		inputName := "input"
		if len(inputDatasets) > 0 {
			inputName = fmt.Sprintf("d%d", inputDatasets[0].GetId())
		}

		connect(canvas, prevSourcePoint, stepGroupOutPoint, inputName)

		stepGroupOutPoint = doStepGroup(canvas, stepGroupOutPoint, status, stepGroup)
		prevSourcePoint = stepGroupOutPoint

		stepGroupOutPoint.y += LineLength

	}

	endInPoint := point{width / 2, prevSourcePoint.y + LineLength}
	doState(canvas, endInPoint, "end")

	//connect(canvas, startOut, endIn)
	connect(canvas, prevSourcePoint, endInPoint, "output")

	canvas.End()

	return svgWriter.String()
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
		stepOut = doStep(canvas, stepOut, step)
	}

	rectstyle := fmt.Sprintf("stroke:%s;stroke-width:1;fill:%s", "black", "none")

	canvas.Rect(x, y, w, h, rectstyle)

	output.x = input.x
	output.y = input.y + h

	return
}

func doStep(canvas *svg.SVG, input point, step *pb.FlowExecutionStatus_Step) (output point) {
	output.x = input.x
	output.y = input.y + HightStep + SmallMargin

	x, y := input.x-WidthStep/2+SmallMargin, input.y+SmallMargin
	w, h := WidthStep-2*SmallMargin, HightStep
	fs := 14
	w2 := 3

	name := step.GetName()
	canvas.Rect(x, y, w, h, fmt.Sprintf("stroke:%s;stroke-width:1;fill:%s", "black", "gray"))
	canvas.Gstyle(fmt.Sprintf("font-size:%dpx", fs))
	canvas.Text(x+w2, y+HightStep-SmallMargin/2, name, "stroke:black;baseline-shift:50%")
	canvas.Gend()

	return
}

func getStepInputDatasets(status *pb.FlowExecutionStatus, step *pb.FlowExecutionStatus_Step) (inputs []*pb.FlowExecutionStatus_Dataset) {
	for _, datasetId := range step.GetInputDatasetId() {
		inputs = append(inputs, status.GetDataset(datasetId))
	}
	return
}
