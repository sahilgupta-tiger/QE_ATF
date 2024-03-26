import PropTypes from 'prop-types';
import { useFormContext, Controller } from 'react-hook-form';
import { TextField } from '@mui/material';

RHFTextField.propTypes = {
    name: PropTypes.string,
    // size: PropTypes.string,
};

export default function RHFTextField({ name, size, ...other }) {
    const { control } = useFormContext();

    return (
        <Controller
            name={name}
            control={control}
            render={({ field, fieldState: { error } }) => (
                <TextField
                    {...field}
                    onChange={(event) => {
                        if (other.type === 'number') {
                            field.onChange(+event.target.value)
                        } else {
                            field.onChange(event.target.value)
                        }
                    }}
                    //   fullWidth
                    size="small"
                    variant="outlined"
                    // color="#E3E3E3"
                    sx={{font:"normal normal normal 12px/13px Articulat CF"}}
                    value={typeof field.value === 'number' && field.value === 0 ? '' : field.value}
                    error={!!error}
                    helperText={error?.message}
                    {...other}

                />
            )}
        />
    );
}
