import { Box, Card, CardContent, Divider, Grid, Step, StepLabel, Stepper, gridClasses, styled } from "@mui/material";
import { useNavigate } from "react-router-dom";
import "./Mapping.css"
import { useState } from "react";
import StepConnector, { stepConnectorClasses } from '@mui/material/StepConnector';
import FormProvider from "../../Common/FormProvider";
import { useForm } from "react-hook-form";
import RHFTextField from "../../Common/RHFTextField";
import RHFSelectField from "../../Common/RHFSelectField";
import { DataGrid } from "@mui/x-data-grid";
import { Mappingactive2 } from "../Extraction/Data";
import MUIModal from "../../Common/MuiModal";

export default function NewMappingCreation() {
    const navigate = useNavigate();
    const [activeStep, setActiveStep] = useState(0);
    const [rows, setRows] = useState(Mappingactive2);
    const [open, setOpen] = useState(false)
    const methods = useForm();
    const { handleSubmit, reset } = methods;

    const onSubmit = (data) => {
        console.log(data)
    }

    const handleNext = () => {
        setActiveStep((prevActiveStep) => prevActiveStep + 1);
    };

    const handleBack = () => {
        setActiveStep((prevActiveStep) => prevActiveStep - 1);
    };

    const handleReset = () => {
        setActiveStep(0);
    };

    const handleExit = () => {
        setOpen(true)
    }
    const handleClose = () => {
        setOpen(false)
    }

    const steps = [
        {
            label: 'Mapping Configuration',
        },
        {
            label: 'Schema',

        },
        {
            label: 'Target Mapping',
        },
    ];

    const ColorlibConnector = styled(StepConnector)(({ theme }) => ({
        [`&.${stepConnectorClasses.alternativeLabel}`]: {
            top: 22,
        },
        [`&.${stepConnectorClasses.active}`]: {
            [`& .${stepConnectorClasses.line}`]: {
                background: '#F7901D',
            },
        },
        [`&.${stepConnectorClasses.completed}`]: {
            [`& .${stepConnectorClasses.line}`]: {
                background: '#F7901D',
            },
        },
        [`& .${stepConnectorClasses.line}`]: {
            height: 8,
            border: 0,
            width: 3,
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            backgroundColor:
                theme.palette.mode === 'dark' ? theme.palette.grey[800] : '#eaeaf0',
            borderRadius: 1,
        },
    }));

    const ColorlibStepIconRoot = styled('div')(({ theme, ownerState }) => ({
        backgroundColor: theme.palette.mode === 'dark' ? theme.palette.grey[700] : '#ccc',
        zIndex: 1,
        color: '#fff',
        width: 20,
        height: 20,
        display: 'flex',
        borderRadius: '50%',
        justifyContent: 'center',
        alignItems: 'center',
        ...(ownerState.active && {
            background: '#F7901D',
        }),
        ...(ownerState.completed && {
            background: '#F7901D',
        }),
    }));
    function ColorlibStepIcon(props) {
        const { active, completed, className } = props;

        // const icons = {
        //   1: <SettingsIcon />,
        //   2: <GroupAddIcon />,
        //   3: <VideoLabelIcon />,
        // };

        return (
            <ColorlibStepIconRoot ownerState={{ completed, active }} className={className}>
                {/* <div className="dot"></div> */}
            </ColorlibStepIconRoot>
        );
    }
    const Connection_type = ["simple", "complex"];
    const handleEditCellChange = (params) => {
        const { id, field, props } = params;
        const updatedRows = rows.map((row) =>
            row.id === id ? { ...row, [field]: props.value } : row
        );
        setRows(updatedRows);

        // Log updated JSON data
        console.log(updatedRows);
    };

    const columns = [
        { field: 'Tgt_Table_Type', headerName: 'Tgt Table Type', width: 160, editable: true, headerClassName: 'super-app-theme--header', },
        { field: 'Tgt_Table_Name', headerName: 'Tgt Table Name', width: 160, editable: true, headerClassName: 'super-app-theme--header', },
        { field: 'Tgt_Column_Name', headerName: 'Tgt Column Name', width: 160, editable: true, headerClassName: 'super-app-theme--header', },
        { field: 'Src_Table_Type', headerName: 'Src Table Type', width: 160, editable: true, headerClassName: 'super-app-theme--header', },
        { field: 'Src_Column_Name', headerName: 'Src Column Name', width: 160, editable: true, headerClassName: 'super-app-theme--header', },
        { field: 'Transformation_Type', headerName: 'Transformation Type', width: 160, editable: true, headerClassName: 'super-app-theme--header', },
        { field: 'filterout_nulls', headerName: 'Filterout Nulls', width: 160, editable: true, headerClassName: 'super-app-theme--header', },
        { field: 'select_sql_expression', headerName: 'Select SQL Expression', width: 160, editable: true, headerClassName: 'super-app-theme--header', },
        { field: 'filter_sql_expression', headerName: 'Filter SQL Expression', width: 160, editable: true, headerClassName: 'super-app-theme--header', },
        { field: 'lookup_table_name', headerName: 'Lookup Table Name', width: 160, editable: true, headerClassName: 'super-app-theme--header', },
        { field: 'lookup_column_name', headerName: 'Lookup Column Name', width: 160, editable: true, headerClassName: 'super-app-theme--header', },
        { field: 'lookup_return_expression', headerName: 'Lookup Return Expression', width: 200, editable: true, headerClassName: 'super-app-theme--header', },
        { field: 'remove_duplicate_partition', headerName: 'Remove Duplicate Partition By', width: 200, editable: true, headerClassName: 'super-app-theme--header', },
        { field: 'remove_duplicate_order', headerName: 'Remove Duplicate Order By Sequence', width: 200, editable: true, headerClassName: 'super-app-theme--header', },
        { field: 'remove_duplicate_order_type', headerName: 'Remove Duplicate Order By Type', width: 200, editable: true, headerClassName: 'super-app-theme--header', },

    ];

    const StyledDataGrid = styled(DataGrid)(({ theme }) => ({
        border: 0,
        color:
            theme.palette.mode === 'light' ? 'rgba(0,0,0,.85)' : 'rgba(255,255,255,0.85)',
        fontFamily: [
            'Articulat CF'
        ].join(','),
        WebkitFontSmoothing: 'auto',
        letterSpacing: 'normal',
        '& .MuiDataGrid-columnsContainer': {
            backgroundColor: theme.palette.mode === 'light' ? '#fafafa' : '#1d1d1d',
        },
        '& .MuiDataGrid-iconSeparator': {
            display: 'none',
        },
        '& .MuiDataGrid-columnHeader, .MuiDataGrid-cell': {
            borderRight: `1px solid ${theme.palette.mode === 'light' ? '#E3E3E3' : '#303030'
                }`,
            //   border:'5px',
        },
        '& .MuiDataGrid-columnsContainer, .MuiDataGrid-cell': {
            borderBottom: `1px solid ${theme.palette.mode === 'light' ? '#E3E3E3' : '#303030'
                }`,
            //   border:'5px',
        },
        '& .MuiDataGrid-cell': {
            color:
                theme.palette.mode === 'light' ? 'rgba(0,0,0,.85)' : 'rgba(255,255,255,0.65)',
            borderRadius: '10px',
        },
        '& .MuiPaginationItem-root': {
            borderRadius: 5,
        },
        // ...customCheckbox(theme),
    }));
    return (
        <div className='main-container-layout'>
            <div className="heading image" onClick={() => navigate('/mapping_config')}><img src='/img/asset_19.svg' alt='back' className='image' /> New Mapping Config</div>
            <Card >
                <CardContent>
                    <div>
                        <Grid container spacing={2}>
                            <Grid item xs={3}>
                                <div className='stepper-container'>
                                    <div className='second-container-stepper'>
                                        <Box sx={{ maxWidth: 400 }}>
                                            <Stepper activeStep={activeStep} orientation="vertical" connector={<ColorlibConnector />}>
                                                {steps.map((step, index) => (
                                                    <Step key={step.label} sx={{ color: '#F7901D' }}>
                                                        <StepLabel StepIconComponent={ColorlibStepIcon}>
                                                            <div className='step_Label'>{step.label}</div>
                                                        </StepLabel>
                                                    </Step>
                                                ))}
                                            </Stepper>
                                        </Box>
                                    </div>
                                </div>
                            </Grid>
                            <Grid item xs={9} >
                                <div className='freeze-container2'>
                                    {activeStep === 0 &&
                                        <>
                                            <div className='card-container'>
                                                <div className="form-sub-heading">General Details</div>
                                            </div>
                                            <Divider /> </>}
                                    <FormProvider methods={methods} onSubmit={handleSubmit(onSubmit)}>
                                        {activeStep === 0 &&
                                            <>
                                                <div className='form-data'>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Mapping Name</div>

                                                        <RHFTextField name='mapping_name' className='form-inputbox-text' />
                                                    </div>

                                                    <div className='form-box'>
                                                        <div className='form-text'>Mapping Description</div>
                                                        <RHFTextField name='mapping_discription' className='form-inputbox2-text' />
                                                    </div>

                                                </div>
                                                <div className='form-data'>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Mappping Extended Properties</div>
                                                        <RHFTextField name='mapping_extended' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Mappping Extended Properties2</div>
                                                        <RHFTextField name='mapping_extended2' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Mappping Extended Properties3</div>
                                                        <RHFTextField name='mapping_extended3' className='form-inputbox-text' />
                                                    </div>
                                                </div>
                                                <div className='card-container'>
                                                    <div className="form-sub-heading">Source Details</div>

                                                </div>
                                                <Divider />

                                                <div className='form-data'>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Source Alias Name</div>
                                                        <RHFTextField name='source_name' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Source Connection Name</div>
                                                        <RHFTextField name='source_connection' className='form-inputbox-text' />
                                                    </div>

                                                </div>
                                                <div className='form-data'>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Source Connection Type</div>
                                                        <RHFSelectField name='source_connection_type' variant='outlined' className='form-inputbox-text' options={Connection_type.map((val) => ({
                                                            value: val,
                                                            label: val,
                                                        }))} width="220px" />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Source File Format</div>
                                                        <RHFSelectField name='source_file' variant='outlined' className='form-inputbox-text' options={Connection_type.map((val) => ({
                                                            value: val,
                                                            label: val,
                                                        }))} width="220px" />
                                                    </div>
                                                </div>
                                                <div className='form-data'>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Source File Path</div>
                                                        <RHFTextField name='source_file_path' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Source File Name</div>
                                                        <RHFTextField name='source_file_name' className='form-inputbox-text' />
                                                    </div>
                                                </div>
                                                <div className='form-data'>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Source File has Header</div>
                                                        <RHFTextField name='source_header' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Source File Delimiter</div>
                                                        <RHFTextField name='source_delimiter' className='form-inputbox-text' />
                                                    </div>
                                                </div>
                                                <div className='form-data'>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Source Database Schema Name</div>
                                                        <RHFTextField name='source_schema' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Source Database Table Name</div>
                                                        <RHFTextField name='source_table' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Source Time Stamp Format</div>
                                                        <RHFTextField name='source_timestamp' className='form-inputbox-text' />
                                                    </div>
                                                </div>

                                                <div className='card-container'>
                                                    <div className="form-sub-heading">Target Details</div>

                                                </div>
                                                <Divider />

                                                <div className='form-data'>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Target Alias Name</div>
                                                        <RHFTextField name='target_name' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Target Connection Name</div>
                                                        <RHFTextField name='target_connection' className='form-inputbox-text' />
                                                    </div>

                                                </div>
                                                <div className='form-data'>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Target Connection Type</div>
                                                        <RHFSelectField name='target_connection' variant='outlined' className='form-inputbox-text' options={Connection_type.map((val) => ({
                                                            value: val,
                                                            label: val,
                                                        }))} width="220px" />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Target Datatype</div>
                                                        <RHFSelectField name='target_datatype' variant='outlined' className='form-inputbox-text' options={Connection_type.map((val) => ({
                                                            value: val,
                                                            label: val,
                                                        }))} width="220px" />
                                                    </div>
                                                </div>
                                                <div className='form-data'>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Target File Format</div>
                                                        <RHFSelectField name='target_file_format' variant='outlined' className='form-inputbox-text' options={Connection_type.map((val) => ({
                                                            value: val,
                                                            label: val,
                                                        }))} width="220px" />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Target File Description</div>
                                                        <RHFTextField name='target_discription' className='form-inputbox2-text' />
                                                        {/* <input type='text' className='form-inputbox2' /> */}
                                                    </div>
                                                </div>
                                                <div className='form-data'>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Target File Path</div>
                                                        <RHFTextField name='target_file_path' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Target File Name</div>
                                                        <RHFTextField name='target_file_name' className='form-inputbox-text' />
                                                    </div>
                                                </div>

                                                <div className='form-data'>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Target File has Header</div>
                                                        <RHFTextField name='target_header' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Target File Delimiter</div>
                                                        <RHFTextField name='target_delimiter' className='form-inputbox-text' />
                                                    </div>
                                                </div>
                                                <div className='form-data'>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Target Database Schema Name</div>
                                                        <RHFTextField name='target_schema' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Target Database Table Name</div>
                                                        <RHFTextField name='target_table' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Target Time Stamp Format</div>
                                                        <RHFTextField name='target_timestamp' className='form-inputbox-text' />
                                                    </div>
                                                </div>

                                                <div className='card-container'>
                                                    <div className="form-sub-heading">Other Details</div>

                                                </div>
                                                <Divider />

                                                <div className='form-data'>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Trim White Spaces</div>
                                                        <RHFTextField name='trim_space' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Remove Duplicates</div>
                                                        <RHFTextField name='remove_deplicate' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Enable Blacklisting</div>
                                                        <RHFTextField name='enable_blacklisting' className='form-inputbox-text' />
                                                    </div>

                                                </div>
                                                <div className='form-data'>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Target Load Strategy</div>
                                                        <RHFTextField name='target_load' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Truncate Target Before Load</div>
                                                        <RHFTextField name='truncate_target' className='form-inputbox-text' />
                                                    </div>
                                                </div>
                                                <div className='form-data'>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Preprocessor Function1</div>
                                                        <RHFTextField name='preprocessor1' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Preprocessor Function2</div>
                                                        <RHFTextField name='preprocessor2' className='form-inputbox-text' />
                                                    </div>
                                                    <div className='form-box'>
                                                        <div className='form-text'>Preprocessor Function3</div>
                                                        <RHFTextField name='preprocessor3' className='form-inputbox-text' />
                                                    </div>

                                                </div>
                                            </>
                                        }
                                        {activeStep === 1 &&
                                            <StyledDataGrid
                                                rows={rows}
                                                columns={columns}
                                                rowHeight={40}
                                                headerHeight={40}
                                                onEditCellChange={handleEditCellChange}
                                                sx={{
                                                    border: 0,
                                                    [`& .${gridClasses.row}`]: {
                                                        bgcolor: (theme) =>
                                                            theme.palette.mode === 'light' ? "#FFF" : '#707070',
                                                        font: 'normal normal normal 14px/50px Articulat CF',
                                                        fontWeight: 400,
                                                        color: '#707070',
                                                        height: '20px',
                                                        border: '2px solid grey',
                                                        borderRadius: '5px'
                                                    },
                                                    '& .super-app-theme--header': {
                                                        backgroundColor: '#F4F4F4',
                                                        height: '10px',
                                                        font: 'normal normal bold 14px/50px Articulat CF',
                                                        fontWeight: 700,

                                                    },
                                                }} />}
                                        {activeStep === 2 &&
                                            <StyledDataGrid
                                                rows={rows}
                                                columns={columns}
                                                pageSize={5}
                                                rowHeight={40}
                                                headerHeight={40}
                                                rowsPerPageOptions={[5, 10, 20]}
                                                onEditCellChange={handleEditCellChange}
                                                sx={{
                                                    border: 0,
                                                    [`& .${gridClasses.row}`]: {
                                                        bgcolor: (theme) =>
                                                            theme.palette.mode === 'light' ? "#FFF" : '#707070',
                                                        font: 'normal normal normal 14px/50px Articulat CF',
                                                        fontWeight: 400,
                                                        color: '#707070',
                                                        height: '20px',
                                                        border: '2px solid grey',
                                                        borderRadius: '5px'
                                                    },
                                                    '& .super-app-theme--header': {
                                                        backgroundColor: '#F4F4F4',
                                                        height: '10px',
                                                        font: 'normal normal bold 14px/50px Articulat CF',
                                                        fontWeight: 700,

                                                    },
                                                }} />
                                        }
                                    </FormProvider>
                                    <MUIModal open={open} handleClose={handleClose} hideClose={true} children={<div className="mui" >
                                        <h3>Are you sure you want to leave this page?</h3>
                                        <div style={{ color: '#707070', marginTop: '5px' }}>Your unsaved data will be lost</div>
                                    </div>}
                                        title={`Confirm Navigation`} action={<>
                                            <button className='back-btn' onClick={handleClose}>Cancel</button>
                                            <button className='save-btn' onClick={() => navigate("/mapping_config")} >Confirm</button>
                                        </>} />
                                </div>
                                <div style={{ position: 'relative' }}>
                                    <div className='divider'>
                                        <Divider />
                                    </div>
                                    <div className='butn'>
                                        <button className='back-btn' onClick={handleExit}>Back</button>
                                        {activeStep === 0 ? <button className='disable-btn'>Prev</button> :
                                            <button className='create-btn' onClick={handleBack}>Prev</button>}
                                        {activeStep === 2 ? <button className='save-btn' type='submit' >Save</button> :
                                            <button className='save-btn' onClick={handleNext}>Next</button>}
                                    </div>
                                </div>
                            </Grid>
                        </Grid>
                    </div>
                </CardContent>
            </Card>
        </div >
    )
}