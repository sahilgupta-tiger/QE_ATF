import * as React from 'react';
import { Stack } from '@mui/material'
import { grey } from '@mui/material/colors';
import { DataGrid, gridClasses, GridToolbar } from '@mui/x-data-grid';

export default function CreateDataGrid(props) {
  let {
    rows,
    columns,
    columnVisibilityModel,
    disableColumnMenu,
    pagination,
    paginationMode,
    disableColumnSelector,
    disableColumnFilter,
    rowSpacing = true,
    rowCount,
    page,
    setPage,
    sizePerPage,
    setSizePerPage,
    getRowClassName,
    hideCountNumber,
    disableRowSelectionOnClick,
    // boxShadow = 1,
    // ...restProps
  } = props;

  const rowsPerPageOptions = [10, 15, 20, 50, 100]

  const getRowSpacing = React.useCallback((params) => {
    return {
      top: params.isFirstVisible ? 0 : 0,
      bottom: params.isLastVisible ? 0 : 0,
    };
  }, []);

  return (
    <DataGrid
      columns={columns}
      fullwidth
      getRowHeight={() => '30px'}
      rows={rows}
      disableSelectionOnClick={true}
      rowCount={rowCount}
      paginationMode={paginationMode ? paginationMode : 'server'}
      rowsPerPageOptions={rowsPerPageOptions}
      pagination={pagination ? pagination : true}
      columnVisibilityModel={columnVisibilityModel ? columnVisibilityModel : {}}
      // hideFooterPagination={hideFooterPagination ? hideFooterPagination : false}
      disableColumnFilter={disableColumnFilter ? disableColumnFilter : true}
      disableColumnMenu={disableColumnMenu ? disableColumnMenu : true}
      disableColumnSelector={disableColumnSelector ? disableColumnSelector : true}
      onPageSizeChange={setSizePerPage}
      onPageChange={setPage}
      autoHeight={true}
      getRowSpacing={rowSpacing ? getRowSpacing : ''}
      page={page}
      pageSize={sizePerPage}
      getRowId={props.getRowId}
      getRowClassName={getRowClassName}
      disableRowSelectionOnClick={disableRowSelectionOnClick ? disableRowSelectionOnClick : false}
      sx={{
        border: 0,
        // font: 'normal normal normal 14px/50px Articulat CF',
        [`& .${gridClasses.row}`]: {
          bgcolor: (theme) =>
            theme.palette.mode === 'light' ? "#FFF" : '#707070',
          font: 'normal normal normal 14px/50px Articulat CF',
          fontWeight: 400,
          color: '#707070',
          height: '30px',
        },
        '& .super-app-theme--header': {
          backgroundColor: '#F4F4F4',
          // borderRadius: '5px',
          height: '20px',
          font: 'normal normal bold 14px/50px Articulat CF',
          fontWeight: 700,

        },
        // '.MuiTablePagination-displayedRows': {
        //   display: (hideCountNumber) ? 'none' : 'false', // 👈 to hide huge pagination number
        // },
      }}
      components={getComponents(props.exportToExcel || false)}
      localeText={{
        MuiTablePagination: {
          labelDisplayedRows: ({ from, to, count }) => {
            if (hideCountNumber) {
              return from + "-" + to + " of many";
            }
            return `${from} - ${to} of ${count === Number.MAX_VALUE ? "many" : count
              }`
          }
        }
      }}
    />

  );
}

function getComponents(exportToExcel) {
  let options = {
    NoRowsOverlay: () => (
      <Stack height="100%" alignItems="center" justifyContent="center">
        Uh ho! Thats all folks!
      </Stack>
    ),
    NoResultsOverlay: () => (
      <Stack height="100%" alignItems="center" justifyContent="center">
        Uh ho! Thats all folks!
      </Stack>
    )
  }

  if (exportToExcel) {
    options.Toolbar = GridToolbar
  }

  return options

}