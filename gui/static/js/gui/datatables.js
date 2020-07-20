function createTable(dataset,columns,id){
    $.fn.dataTable.ext.errMode = 'none';
     var tableref=$(id).DataTable( {
        "lengthMenu": [ [ 10, 25, 50,100, -1], [ 10, 25, 50,100,"All"] ],
        scrollY:        '60vh',
        scrollCollapse: true,
        paging:         true,
        scrollX: true,
        data: dataset,
        colReorder: true,
        select:true,
        columns: columns,
        errMode: "throw",
        responsive: true,
        dom: 'lBfrtip',
        buttons: [
            'csv',{
                extend: 'colvis',
                collectionLayout: 'fixed two-column'
            }
        ]
    } );
     tableref.on( 'select', function ( e, dt, type, indexes ) {
        if ( type === 'row' ) {
            var data = tableref.rows( indexes ).data().toArray()[0];
            ztfid=data[ztfidx.idx]

            document.dispatchEvent(new CustomEvent("itemSelected",{"detail":ztfid}))
        }
    } );
     return tableref
}