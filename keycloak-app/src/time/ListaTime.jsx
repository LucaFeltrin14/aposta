import { useKeycloak } from "@react-keycloak/web";
import { useEffect, useState } from "react";


function ListaTime() {

    
    const { keycloak, initialized } = useKeycloak();
    const [data, setData] = useState([]);

    useEffect(() => {
        if (initialized && keycloak.authenticated) {
          fetch('http://localhost:8080/time', {
            method: 'GET',
            headers: {
              Authorization: `Bearer ${keycloak.token}`, // Adiciona o token ao cabeçalho
            },
          }).then(response => {
            return response.json()
          }).then(data => {
            setData(data)
          })
        }
      }, [initialized, keycloak]);


    return (
        <div>

            <table>
                <tr>
                <td>Nome</td>
                <td>Identificador</td>
                </tr>
                {data.map((time, index) => {

                return <tr>
                    <td>{time.nome}</td>
                    <td>{time.identificador}</td>
                </tr>

                })}

            </table>

        </div>
    )

}

export default ListaTime;