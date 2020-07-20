var last_sn="SELECT * FROM objects as o LEFT JOIN crossmatch ON crossmatch.oid = o.oid WHERE (o.classrf NOT IN (10,11,12,13,14) and o.classearly<>19) OR o.lasair_class like '%SN%'";
var last_candidates="SELECT o.oid,o.meanra,o.meandec, o.nobs,o.min_magap_r,o.min_magap_g,o.lasair_class,early_cls.name AS early_type,late_cls.name AS late_type,sl.score,o.tns_name,o.pclassearly,o.pclassrf FROM objects as o LEFT JOIN class AS early_cls ON early_cls.id=o.classearly LEFT JOIN class AS late_cls ON late_cls.id=o.classrf where o.lastmjd >= mjdnow()-2 and o.classearly=19 and o.pclassearly>=0.5 and o.nobs>=3 limit 1000;";

function getQuery(query_str) {

    console.log(query_str)
    var request = $.ajax({
        method: "POST",
        url: lastcandidates_url,
        data: {query: query_str},
        dataType: "json",
    });
    request.done(function (result) {

        if (result.length <= 0) {
            $("#error-message").removeClass("d-none").addClass("d-block")
            $("#loading").removeClass("d-flex").addClass("d-none")
        } else {
            $("#error-message").removeClass("d-block").addClass("d-none")
        }


        for (let col in result.columns) {
            if (ztfidx.name == result.columns[col]) {
                ztfidx.idx = col;
            } else if (raidx.name == result.columns[col]) {
                raidx.idx = col;
            } else if (decidx.name == result.columns[col]) {
                decidx.idx = col;
            }
            columns.push({title: result.columns[col]});
        }

        dataset = result.data;


        var reftable = createTable(dataset, columns, "#results-table")
        references["table"] = reftable
        document.dispatchEvent(new CustomEvent("start-app"));
        $("#loading").removeClass("d-flex").addClass("d-none");
        $("#main-content").removeClass("invisible").addClass("visible");


    });

}
var columns=[]
var data=[]
var dataset = []
$(document).ready(function () {

    $("#queries_str").change(function(val){
        $("#query").val(eval($(this).val()));
    });
    $("#getquery").submit(function( event ) {
        event.preventDefault();
        event.stopPropagation();
        $("#error-message").removeClass("d-block").addClass("d-none");
        $("#loading").removeClass("d-none").addClass("d-flex");
        getQuery($("#query").val());

    });

});