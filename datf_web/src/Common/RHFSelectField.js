import PropTypes from 'prop-types';
import { useFormContext, Controller } from 'react-hook-form';
import { FormControl, Select, MenuItem, FormHelperText, InputLabel } from '@mui/material';

RHFSelectField.propTypes = {
  name: PropTypes.string,
  label: PropTypes.string,
  options: PropTypes.array
};

export default function RHFSelectField({ name, options, variant, label, width = 150, ...other }) {
  const { control } = useFormContext();
  const uniqueId = (new Date()).getTime();

  return (
    <Controller
      name={name}
      control={control}
      render={({ field, fieldState: { error } }) => (
        <FormControl variant={variant || "filled"} error={!!error} sx={{ minWidth: width ,boxShadow:'none'}} size='medium'>
          <InputLabel id={`mui-select-${uniqueId}`}>{label}</InputLabel>
          <Select
            {...field}
            labelId={`mui-select-${uniqueId}`}
            value={typeof field.value === 'number' && field.value === 0 ? '' : field.value}
            fullWidth={true}
            id="demo-select-small"
            
            {...other}
          >
            {/* <MenuItem value="">
              <em>None</em>
            </MenuItem> */}
            {options.map((e, i) => <MenuItem key={i} value={e.value}>{e.label}</MenuItem>)}
          </Select>
          <FormHelperText>{error?.message}</FormHelperText>
        </FormControl>
      )}
    />
  );
}
