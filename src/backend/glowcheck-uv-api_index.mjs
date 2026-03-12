export const handler = async (event) => {

    const lat    = event.queryStringParameters?.lat || '-37.81';
    const lon    = event.queryStringParameters?.lon || '144.96';
    const apiKey = process.env.OWM_API_KEY;
  
    try {
      const url = `https://api.openweathermap.org/data/3.0/onecall`
                + `?lat=${lat}&lon=${lon}`
                + `&appid=${apiKey}`
                + `&units=metric`
                + `&exclude=minutely,hourly,daily,alerts`;
  
      const response = await fetch(url);
      const data     = await response.json();
  
      const uv   = data.current?.uvi  ?? null;
      const temp = data.current?.temp ?? null;
  
      return {
        statusCode: 200,
        headers: {
          'Content-Type':                'application/json',
          'Access-Control-Allow-Origin': '*',
        },
        body: JSON.stringify({ uv, temp, lat, lon }),
      };
  
    } catch (err) {
      return {
        statusCode: 500,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ error: err.message }),
      };
    }
  };