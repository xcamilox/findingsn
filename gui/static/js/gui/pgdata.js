var columns=[]
var data=[]
var dataset = []

$(document).ready(function () {
   $("#updatechat").submit(function( event ) {
        event.preventDefault();
        event.stopPropagation();
        updatePlots()

    });

    var opts = {}

    if (days != null) {
        opts["days"] = days
    }
    if (filter != null) {
        opts["filter"] = filter
    }
    var request = $.ajax({
        method: "POST",
        url: lastcandidates_url,
        data: opts,
        dataType: "json",
    })
    request.fail(function(result){
        console.log("error respond")
    });
    request.done(function (result) {
        if (result.length <= 0) {
            $("#error-message").removeClass("d-none").addClass("d-block")
            $("#loading").removeClass("d-flex").addClass("d-none")
        } else {
            $("#error-message").removeClass("d-block").addClass("d-none")
        }


        for(let col in result.columns){
            if(ztfidx.name==result.columns[col]){
                ztfidx.idx=col;
            }else if (raidx.name==result.columns[col]){
                raidx.idx=col;
            }else if (decidx.name==result.columns[col]){
                decidx.idx=col;
            }
            columns.push({title:result.columns[col]});
        }
        //columns =  result.columns;
        dataset =  result.data;


        var reftable = createTable(dataset, columns, "#results-table")
        references["table"] = reftable
        document.dispatchEvent(new CustomEvent("start-app"));
        $("#loading").removeClass("d-flex").addClass("d-none")
        $("#main-content").removeClass("invisible").addClass("visible")


    });
});