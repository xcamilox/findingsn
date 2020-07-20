/*
This class create used and extend the plotly js
 */

class ChartManager {

    charType = ["scatter", "bar", "pie"];

    charMode = ["lines", "markers"]

    refElement = ""
    create_plot_btn = "create-plot-btn"


    currentplots = []

    constructor(idElement) {
        this.refElement = idElement
        $(this.refElement).tabs();
        this.createplot_form_btn = document.getElementById(this.create_plot_btn)
        this.cancel_form_btn = document.getElementById(this.cancel_plot_btn)
        var ref = this;


        this.createplot_form_btn.onclick = function (event) {
            event.stopImmediatePropagation();
            event.preventDefault();
            ref.createPlot()
        }
        this.fillOptions("#xaxis")
        this.fillOptions("#yaxis")


    }

    fillOptions(field_id) {
        if (columns != null && columns.length > 0) {

            var xaxis = document.querySelector(field_id);
            for (let column in columns) {
                let optionx = document.createElement("option")
                optionx.value = column
                optionx.text = columns[column].title
                xaxis.add(optionx)
            }

        }
    }

    get chartTypes() {
        return this.charType
    }

    get formPlotTrace() {
        return "<form>\n" +
            "            <div class=\"row\">\n" +
            "                <div class=\"col\">\n" +
            "                    <div class=\"form-row align-items-center\">\n" +
            "                        <div class=\"form-group\">\n" +
            "                            <label>X Axis</label>\n" +
            "                            <select class=\"form-control xaxis\" >\n" +
            "                            </select>\n" +
            "                        </div>\n" +
            "                        <div class=\"form-check form-check-inline mt-3 ml-3\">\n" +
            "                            <input class=\"form-check-input flipx\" type=\"checkbox\" >\n" +
            "                            <label class=\"form-check-label\">Flip</label>\n" +
            "                        </div>\n" +
            "                    </div>\n" +
            "                    <div class=\"form-row align-items-center\">\n" +
            "                        <div class=\"form-group\">\n" +
            "                            <label>Y Axis</label>\n" +
            "                            <select class=\"form-control yaxis\">\n" +
            "                            </select>\n" +
            "                        </div>\n" +
            "                        <div class=\"form-check form-check-inline mt-3 ml-3\">\n" +
            "                            <input class=\"form-check-input flipy\" type=\"checkbox\" >\n" +
            "                            <label class=\"form-check-label\">Flip</label>\n" +
            "                        </div>\n" +
            "                    </div>\n" +
            "                   <div class=\"align-items-center\">\n" +
            "                       <div class='row'>\n" +
            "                           <input class=\"jscolor\" value='FF0022'>\n" +
            "                       </div>\n" +
            "                   <div class='row'>\n" +
            "                       <button type=\"button\" class=\"btn btn-primary\">update</button> \n" +
            "                   </div>\n" +
            "              </div>\n" +
            "            </div>\n" +
        "            </div>\n" +
        "        </form>";

    }

    get chartModes() {
        return this.charMode
    }

    basePlot(refId) {

        refId = refId.replace(" ", "")
        this.currentplots.push(refId)
        refId = "plot-" + (this.currentplots.length - 1);

        //create main containers for tabs
        const container = document.createElement("div")
        container.classList.add("row")

        const plot_container = document.createElement("div")
        const tools_container = document.createElement("div")
        plot_container.classList.add("col-", "plot-container")
        tools_container.classList.add("col-", "tools-container")
        plot_container.innerHTML = '<div  id="' + refId + '-container"></div>'

        tools_container.innerHTML = '<div class="tools-tab container-fluid"><ul><li><a href="#add-trace">+</a></li><li><a href="#trace-0">trace0</a></li></ul><div id="add-trace">' + this.formPlotTrace + '</div><div id="trace-0" data-id="0">' + this.formPlotTrace + '</div>'
        container.appendChild(plot_container)
        container.appendChild(tools_container)
        return container


    }

    createscatter(ref) {
        let item = this.basePlot(ref)
        return item
    }

    addPlot(plot, title) {
        let idtap = title.replace(" ", "")
        let index = (this.currentplots.length - 1);
        $("#" + this.refElement + " > ul").append('<li><a href="#plot-' + index + '"><span>' + title + '</span></a></li>')
        let plot_content = document.createElement("div")
        Object.assign(plot_content, {id: "plot-" + index})
        plot_content.appendChild(plot)
        document.getElementById(this.refElement).appendChild(plot_content)

        $("#plot-" + index + " .tools-tab").tabs();
        $("#plot-" + index + " .tools-tab").tabs("option", "active", 1);
        this.fillOptions("#plot-" + index + " #add-trace .xaxis")
        this.fillOptions("#plot-" + index + " #add-trace .yaxis")
        this.fillOptions("#plot-" + index + " #trace-0 .xaxis")
        this.fillOptions("#plot-" + index + " #trace-0 .yaxis")
        let xaxis = document.getElementById("xaxis").value;
        let yaxis = document.getElementById("yaxis").value;
        $("#plot-" + index + " #trace-0 .xaxis").val(xaxis)
        $("#plot-" + index + " #trace-0 .xaxis").val(yaxis)
        document.querySelector("#plot-" + index + " #add-trace .btn-primary").innerHTML = "add trace";
        let inputbase = document.querySelector("#plot-" + index + " #add-trace .jscolor");
        new jscolor(inputbase)
        let input = document.querySelector("#plot-" + index + " #trace-0 .jscolor");
        new jscolor(input)

        document.querySelector("#plot-" + index + " #trace-0 .flipx").checked = document.getElementById("flipx").checked;
        document.querySelector("#plot-" + index + " #trace-0 .flipy").checked = document.getElementById("flipy").checked;

    }

    createPlot() {

        let type = document.getElementById("plotType").value;
        let title = document.getElementById("plottitle").value;
        let xaxis = document.getElementById("xaxis").value;
        let yaxis = document.getElementById("yaxis").value;

        let reversex = document.getElementById("flipx").checked;
        let reversey = document.getElementById("flipy").checked;

        if (title == "") {
            title = "plot" + this.currentplots.length
        }
        console.log("create", type)
        if (this.charType.includes(type)) {
            let plot = this["create" + type](title)
            this.addPlot(plot, title)
            $("#main-plot-tabs").tabs("refresh");
            $("#main-plot-tabs").tabs("option", "active", this.currentplots.length);
            this.drawPlots(xaxis, yaxis, "plot-" + (this.currentplots.length - 1) + "-container", title, reversex, reversey)

        } else {
            throw "Type chart not supported"

        }
    }


    addTraces(refId) {
        let xcol = $("#xaxis-col").val()
        let ycol = $("#yaxis-col").val()

        let xdata = dataset.map(function (value, index) {
            return value[xcol];
        });
        let ydata = dataset.map(function (value, index) {
            return value[ycol];
        });
        let markers = {
            "ztfid": dataset.map(function (value, index) {
                return value[0];
            })
        }

        Plotly.addTraces(id, {
            y: ydata,
            x: xdata,
            type: "scatter",
            mode: 'markers',
            marker: markers,
            text: dataset.map(function (value, index) {
                return value[0];
            })
        });
    }


    drawPlots(x, y, itemID, title, reversex, reversey) {
        let xdata = dataset.map(function (value, index) {
            return value[x];
        });
        let ydata = dataset.map(function (value, index) {
            return value[y];
        });
        let ztfid = dataset.map(function (value, index) {
            return value[0];
        });

        let xlabel = columns[x].title
        let ylabel = columns[y].title

        let reversexaxes = reversex ? 'reversed' : true;
        let reverseyaxes = reversey ? 'reversed' : true;
        let layout = {
            height: 600,
            xaxis: {
                title: xlabel,
                tickformat: ".2f",
                autorange: reversexaxes,
            },
            yaxis: {
                title: ylabel,
                autorange: reverseyaxes,
                tickformat: ".2f",
                showlegend: false
            },
            title: title,
            showlegend: true,
            displayModeBar: false

        }

        let plot_data = [{
            x: xdata,
            y: ydata,
            type: 'scatter',
            mode: 'markers',
            text: ztfid,
            marker: {
                color: 'FF0022',
                size: 6,
                ztfid: ztfid,
                symbol: "circle"
            }
        }]

        let plot = Plotly.newPlot(document.getElementById(itemID), plot_data, layout, {
            scrollZoom: true,
            displayModeBar: true,
            showlegend: true
        });

        document.getElementById(itemID).on('plotly_click', function (data) {
            for (var i = 0; i < data.points.length; i++) {
                let idtosearch = data.points[i].data.marker.ztfid[data.points[i].pointIndex];
                //data.points[i].data.ref.search(idtosearch ).draw();
                document.dispatchEvent(new CustomEvent("itemSelected", {"detail": idtosearch}))
            }
            ;

        });
        return plot
    }


}

// self executing function here
(function () {
    console.log("listering events");
    document.addEventListener("start-app", function (event) {
        console.log("start app")
        var chartmg = new ChartManager("main-plot-tabs")
        $("#main-plot-tabs").tabs();

    })
})();