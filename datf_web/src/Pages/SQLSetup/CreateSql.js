import { useNavigate } from "react-router-dom";
import "../Overview/overview.css";
import "./sql.css"
import { Card, CardContent, Divider } from "@mui/material";
import FormProvider from "../../Common/FormProvider";
import { useForm } from "react-hook-form";
import RHFTextField from "../../Common/RHFTextField";

export default function CreateSql() {
    const navigate = useNavigate();
    const methods = useForm();
    const { handleSubmit, reset } = methods;

    const onSubmit = (data) => {

    }
    return (
        <div className='main-container-layout'>
            <div className="heading image" onClick={() => navigate('/sql_setup')}><img src='/img/asset_19.svg' alt='back' className='image' /> Create New SQL</div>
            <Card>
                <CardContent>
                    <div className='card-container ht'>
                        <div className="sub-heading">SQL Details</div>
                    </div>
                    <Divider />
                    <FormProvider methods={methods} onSubmit={handleSubmit(onSubmit)}>
                    <div className='form-name'>SQL Name</div>
                    <div><RHFTextField name='sql_name' className='input-box-text'/></div>
                   
                    <div className="form-box2">
                        <div>
                            <div className='form-name'>Source</div>
                            <div><input type='text' className='input-box-text2'/></div>
                        </div>
                        <div>
                            <div className='form-name'>Target</div>
                            <div><input type='text' className='input-box-text2'/></div>
                        </div>

                    </div>
                    <div className='divider'>
                        {/* <div className="form-sub-heading">Other Details</div> */}
                        <Divider />
                    </div>
                    <div className='butn'>
                        <button className='create-btn'>Cancel</button>
                        <button type='onSubmit' className='save-btn'>Save</button>
                    </div>
                    </FormProvider>
                </CardContent>
            </Card>
        </div>
    )
}